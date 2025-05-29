package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/cespare/xxhash"
	"github.com/gobwas/glob"
	"github.com/opencontainers/selinux/pkg/pwalkdir"
	"github.com/schollz/progressbar/v3"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/urfave/cli/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Action interface {
	Execute(path string) error
}

type OmitAction struct{}

func (OmitAction) Execute(path string) error {
	zap.L().Info("Omitting path", zap.String("path", path))
	return nil
}

type DeleteAction struct{}

func (DeleteAction) Execute(path string) error {
	zap.L().Info("Deleting", zap.String("path", path))
	return os.RemoveAll(path)
}

func newAction(action string) Action {
	if action == "" {
		return OmitAction{}
	}

	switch action {
	case "rm":
		fallthrough
	case "delete":
		return DeleteAction{}
	}

	panic(fmt.Errorf("unknown action: %s", action))
}

type searchType int

const (
	onlyFile searchType = iota
	onlyDir
	onlyEmptyDir
	fileAndDir
)

func newSearchType(s string) searchType {
	if s == "file" || s == "f" {
		return onlyFile
	}
	if s == "empty" {
		return onlyEmptyDir
	}
	if s == "d" {
		return onlyDir
	}
	if s == "" {
		return fileAndDir
	}
	panic(fmt.Errorf("unknown search type: %s", s))
}

var cmd = &cli.Command{
	Name:  "gofd",
	Usage: "fd in go",
	Arguments: []cli.Argument{
		&cli.StringArg{Name: "path"},
		&cli.StringArg{Name: "path2", Value: ""},
	},
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "glob",
			Aliases: []string{"g"},
			Value:   "*.*",
		},
		&cli.StringFlag{
			Name:    "action",
			Aliases: []string{"x"},
		},
		&cli.StringFlag{
			Name:    "type",
			Aliases: []string{"t"},
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		path := command.StringArg("path")
		path2 := command.StringArg("path2")
		actionStr := command.String("action")

		if actionStr == "dedup" {
			if path2 == "" {
				return errors.New("please specify another path for deduplicate")
			}
			return deduplicate(path, path2)
		}

		action := newAction(actionStr)
		searchMode := newSearchType(command.String("type"))

		globStr := command.String("glob")
		matcher, err := glob.Compile(globStr)
		if err != nil {
			return err
		}

		return filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}

			switch searchMode {
			case onlyFile:
				if info.IsDir() {
					return nil
				}
			case onlyDir:
				if !info.IsDir() {
					return nil
				}
			case onlyEmptyDir:
				if !info.IsDir() {
					return nil
				}

				ents, err := os.ReadDir(path)
				if err != nil {
					return err
				} else if len(ents) >= 1 {
					return nil
				}

			case fileAndDir:
			default:
			}

			if globStr == "*.*" {
				err = action.Execute(path)
			} else if matcher.Match(path) {
				err = action.Execute(path)
			} else {
				err = nil
			}

			return err
		})
	},
}

type multiHash struct {
	XXHash uint64
	SHA1   [3]uint64
}

func sha1ToArray(buf []byte) [3]uint64 {
	if len(buf) != 20 {
		panic(fmt.Errorf("invalid buffer, len: %d", len(buf)))
	}
	a := [3]uint64{}
	a[0] = binary.BigEndian.Uint64(buf[0:8])
	a[1] = binary.BigEndian.Uint64(buf[8:16])
	a[2] = uint64(binary.BigEndian.Uint32(buf[16:20]))
	return a
}

func hashFile(path string) (multiHash, error) {
	f, err := os.Open(path)
	if err != nil {
		return multiHash{}, err
	}
	defer func() { _ = f.Close() }()

	reader := bufio.NewReader(f)
	x := xxhash.New()
	s := sha1.New()

	buf := make([]byte, 1<<20)
	for {
		var n int
		n, err = reader.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return multiHash{}, err
		}

		s.Write(buf[:n])
		_, err = x.Write(buf[:n])
		if err != nil {
			return multiHash{}, err
		}
	}

	return multiHash{
		XXHash: x.Sum64(),
		SHA1:   sha1ToArray(s.Sum(nil)),
	}, nil
}

func (h *multiHash) Bytes() []byte {
	buf := make([]byte, 32)
	binary.BigEndian.PutUint64(buf[:8], h.XXHash)
	binary.BigEndian.PutUint64(buf[8:16], h.SHA1[0])
	binary.BigEndian.PutUint64(buf[16:24], h.SHA1[1])
	binary.BigEndian.PutUint64(buf[24:32], h.SHA1[2])
	return buf
}

func createHashMap(path string, db *leveldb.DB) error {
	bar := progressbar.NewOptions(-1,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionThrottle(time.Second),
	)
	bar.Describe(fmt.Sprintf("Creating deduplicate hash map, path: %s", path))
	defer func() { _ = bar.Finish() }()

	return pwalkdir.Walk(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		h, err := hashFile(path)
		if err != nil {
			return err
		}

		return db.Put(h.Bytes(), []byte(path), nil)
	})
}

func deduplicate(path1 string, path2 string) error {
	dbPath1, err := os.MkdirTemp("", "gofd-")
	if err != nil {
		return err
	}

	db1, err := leveldb.OpenFile(dbPath1, nil)
	if err != nil {
		return err
	}
	defer func() { _ = db1.Close() }()

	err = createHashMap(path1, db1)
	if err != nil {
		return err
	}

	dbPath2, err := os.MkdirTemp("", "gofd-")
	if err != nil {
		return err
	}

	db2, err := leveldb.OpenFile(dbPath2, nil)
	if err != nil {
		return err
	}
	defer func() { _ = db1.Close() }()

	err = createHashMap(path2, db2)
	if err != nil {
		return err
	}

	iter := db2.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		path := iter.Value()

		_, err = db1.Get(key, nil)
		if err != nil {
			if errors.Is(err, leveldb.ErrNotFound) {
				continue
			} else {
				return err
			}
		} else {
			p := string(path)
			zap.L().Info("Removing file", zap.String("path", p))
			_ = os.Remove(p)
		}
	}
	err = iter.Error()
	iter.Release()
	return err
}

func main() {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, err := config.Build(zap.AddCaller(), zap.AddCallerSkip(1))
	if err != nil {
		panic(err)
	}
	zap.ReplaceGlobals(logger)
	defer func() { _ = logger.Sync() }()

	err = cmd.Run(context.Background(), os.Args)
	if err != nil {
		zap.L().Error("Unexpected error", zap.Error(err))
	}
}
