package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/binary"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/jotfs/fastcdc-go"
	"github.com/negrel/assert"
	"github.com/urfave/cli/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/fanyang89/gofd/pb"
)

var prefixFileEntryPathToID = []byte("pi") // pi:path_hash -> file_entry_id
var prefixFileEntryPathToIDEnd = prefixEndBytes(prefixFileEntryPathToID)

var prefixFileEntry = []byte("fe") // fe:file_id -> file_entry
var prefixFileEntryEnd = prefixEndBytes(prefixFileEntry)

var prefixFileChunk = []byte("fc") // fc:file_id:offset:len -> hash
var prefixFileChunkEnd = prefixEndBytes(prefixFileChunk)

var cdcOption = fastcdc.Options{
	MinSize:     512,
	AverageSize: 16 * 1024,
	MaxSize:     1024 * 1024,
}

var cmdDeduplicateChunk = &cli.Command{
	Name: "chunk",
	Arguments: []cli.Argument{
		&cli.StringArg{Name: "path"},
	},
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "dsn",
			Aliases:  []string{"d"},
			Required: true,
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		rootDir := command.StringArg("path")
		dsn := command.String("dsn")

		db, err := pebble.Open(dsn, &pebble.Options{})
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		cd := NewChunkDeduplicator(db)
		err = filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			zap.L().Info("processing file", zap.String("path", path))
			return cd.ProcessFile(path)
		})
		if err != nil {
			return err
		}

		iter, err := db.NewIter(&pebble.IterOptions{
			LowerBound: prefixFileChunk,
			UpperBound: prefixFileChunkEnd,
		})
		if err != nil {
			return err
		}
		defer func() { _ = iter.Close() }()

		lastHash := ""
		duplicates := make(map[string][]FileChunk)

		for iter.First(); iter.Valid(); iter.Next() {
			assert.True(bytes.HasPrefix(iter.Key(), prefixFileChunk))
			key := iter.Key()
			hash := hex.EncodeToString(key[2 : 2+32])

			if hash != lastHash {
				lastHash = hash
				continue
			}

			l, ok := duplicates[hash]
			fc := FileChunk{
				FileID: binary.BigEndian.Uint64(key[32:40]),
				Offset: binary.BigEndian.Uint64(key[40:48]),
				Length: binary.BigEndian.Uint64(key[48:56]),
			}
			if !ok {
				duplicates[hash] = []FileChunk{fc}
			} else {
				duplicates[hash] = append(l, fc)
			}
		}

		return nil
	},
}

type ChunkDeduplicator struct {
	db              *pebble.DB
	lastFileEntryID atomic.Uint64
}

func NewChunkDeduplicator(db *pebble.DB) *ChunkDeduplicator {
	cd := &ChunkDeduplicator{db: db}
	id, err := cd.getLastFileEntryID()
	if err != nil {
		panic(err)
	}
	cd.lastFileEntryID.Store(id)
	return cd
}

func splitFileIntoChunks(r io.Reader) func(yield func(chunk *fastcdc.Chunk) bool) {
	return func(yield func(chunk *fastcdc.Chunk) bool) {
		chunker, _ := fastcdc.NewChunker(r, cdcOption)

		for {
			chunk, err := chunker.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}

			if !yield(&chunk) {
				break
			}
		}
	}
}

func newKey(prefix []byte, value string) []byte {
	return append(prefix[:], []byte(value)...)
}

func newKeyUInt64(prefix []byte, values ...uint64) []byte {
	buf := make([]byte, len(values)*8)
	for i := 0; i < len(values); i++ {
		lower := i * 8
		upper := lower + 8
		binary.BigEndian.PutUint64(buf[lower:upper], values[i])
	}
	return append(prefix, buf...)
}

func (d *ChunkDeduplicator) nextFileEntryID() uint64 {
	for {
		old := d.lastFileEntryID.Load()
		if d.lastFileEntryID.CompareAndSwap(old, old+1) {
			return old + 1
		}
	}
}

func (d *ChunkDeduplicator) getLastFileEntryID() (uint64, error) {
	iter, err := d.db.NewIter(&pebble.IterOptions{
		LowerBound: prefixFileEntry,
		UpperBound: prefixFileEntryEnd,
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()

	if !iter.SeekLT(prefixFileEntryEnd) {
		return 0, err
	}
	iter.Last()

	key := iter.Key()
	if len(key) != 2+8 || !(key[0] == 'f' && key[1] == 'e') {
		return 0, errors.New("invalid")
	}
	return binary.BigEndian.Uint64(key[2:]), nil
}

func uint64ToByteSlice(value uint64) (buf []byte) {
	buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, value)
	return
}

func (d *ChunkDeduplicator) createFileEntry(path string) (id uint64, err error) {
	hash, err := xxHashFile(path)
	if err != nil {
		return 0, err
	}

	pathHash := xxHashString(path)

	entry := pb.FileEntry{
		Id:   d.nextFileEntryID(),
		Path: path,
		Hash: hash,
	}

	entryBytes, err := proto.Marshal(&entry)
	if err != nil {
		panic(err)
	}

	batch := d.db.NewBatch()
	err = batch.Set(newKeyUInt64(prefixFileEntry, entry.Id), entryBytes, nil)
	if err != nil {
		panic(err)
	}

	err = batch.Set(newKeyUInt64(prefixFileEntryPathToID, pathHash),
		uint64ToByteSlice(entry.Id), nil)
	if err != nil {
		panic(err)
	}

	err = batch.Commit(pebble.NoSync)
	if err != nil {
		return 0, err
	}

	err = batch.Close()
	if err != nil {
		return 0, err
	}

	return entry.Id, nil
}

func (d *ChunkDeduplicator) ensureFileEntryCreated(path string) (id uint64, err error) {
	pathHash := xxHashString(path)
	value, closer, err := d.db.Get(newKeyUInt64(prefixFileEntryPathToID, pathHash))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			id, err = d.createFileEntry(path)
		}
		return
	}

	assert.True(len(value) == 8)
	id = binary.BigEndian.Uint64(value)
	_ = closer.Close()
	return
}

func prefixEndBytes(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}

	end := make([]byte, len(prefix))
	copy(end, prefix)

	for {
		if end[len(end)-1] != byte(255) {
			end[len(end)-1]++
			break
		}

		end = end[:len(end)-1]

		if len(end) == 0 {
			end = nil
			break
		}
	}

	return end
}

var nop []byte

func (d *ChunkDeduplicator) ProcessFile(path string) error {
	fileID, err := d.ensureFileEntryCreated(path)
	if err != nil {
		return err
	}

	start := newKeyUInt64(prefixFileChunk, fileID)
	end := prefixEndBytes(start)
	err = d.db.DeleteRange(start, end, pebble.NoSync)
	if err != nil {
		return err
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	for c := range splitFileIntoChunks(f) {
		hash := sha256ByteSlice(c.Data)
		key := newKeyUInt64(prefixFileChunk,
			hash[0], hash[1], hash[2], hash[3],
			fileID, uint64(c.Offset), uint64(c.Length))
		err = d.db.Set(key, nop, pebble.NoSync)
		if err != nil {
			return err
		}
	}

	err = d.db.Set(nop, nop, pebble.Sync)
	if err != nil {
		return err
	}

	return nil
}

type FileChunk struct {
	FileID uint64
	Offset uint64
	Length uint64
}
