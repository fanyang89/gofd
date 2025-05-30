package main

import (
	"context"
	_ "embed"
	"encoding/binary"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/jotfs/fastcdc-go"
	"github.com/negrel/assert"
	"github.com/urfave/cli/v3"
	"google.golang.org/protobuf/proto"

	"github.com/fanyang89/gofd/pb"
)

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
		return filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			return cd.ProcessFile(path)
		})
	},
}

type ChunkDeduplicator struct {
	db *pebble.DB
}

func NewChunkDeduplicator(db *pebble.DB) *ChunkDeduplicator {
	return &ChunkDeduplicator{
		db: db,
	}
}

var cdcOption = fastcdc.Options{
	MinSize:     4 * 1024,
	AverageSize: 1 * 1024 * 1024,
	MaxSize:     4 * 1024 * 1024,
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

var prefixFileEntryPathToID = []byte("pi") // pi:path_hash -> file_entry_id
var prefixFileEntryPathToIDEnd = []byte("pj")
var prefixFileEntry = []byte("fe") // fe:file_entry_id -> file_entry
var prefixFileEntryEnd = []byte("ff")

func newKey(prefix []byte, value string) []byte {
	return append(prefix[:], []byte(value)...)
}

func newKeyUInt64(prefix []byte, value uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, value)
	return append(prefix, buf...)
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
	lastID, err := d.getLastFileEntryID()
	if err != nil {
		return 0, err
	}

	hash, err := xxHashFile(path)
	if err != nil {
		return 0, err
	}

	pathHash := xxHashString(path)

	entry := pb.FileEntry{
		Id:   lastID + 1,
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

func (d *ChunkDeduplicator) ProcessFile(path string) error {
	fileID, err := d.ensureFileEntryCreated(path)
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
	}

	return nil
}
