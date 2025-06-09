package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/negrel/assert"
	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/fanyang89/gofd/pb"
)

var cmdTool = &cli.Command{
	Name: "tool",
	Commands: []*cli.Command{
		cmdToolChunkDeduplicate,
		cmdAddUtf8Bom,
	},
}

var cmdAddUtf8Bom = &cli.Command{
	Name: "add-utf8-bom",
	Arguments: []cli.Argument{
		&cli.StringArg{Name: "path", Config: trimSpaceConfig},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		path := command.StringArg("path")
		if path == "" {
			return errors.New("path is required")
		}
		path, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		dir := filepath.Dir(path)

		done := false
		w, err := os.CreateTemp(dir, filepath.Base(path))
		if err != nil {
			return err
		}
		defer func() {
			_ = w.Close()
			if !done {
				_ = os.Remove(w.Name())
				return
			}
			if err := os.Rename(w.Name(), path); err != nil {
				zap.L().Error("Rename failed", zap.Error(err),
					zap.String("old", w.Name()), zap.String("new", path))
			}
		}()

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()

		const BOM = "\xef\xbb\xbf"
		r := bufio.NewReader(f)
		b, err := r.Peek(len(BOM))
		if err != nil {
			return err
		}
		if bytes.Equal(b, []byte(BOM)) {
			return nil
		}

		_, err = w.WriteString(BOM) // BOM
		if err != nil {
			return err
		}

		_, err = io.Copy(w, r)
		if err != nil {
			return err
		}

		done = true
		return nil
	},
}

var cmdToolChunkDeduplicate = &cli.Command{
	Name:    "chunk-deduplicate",
	Aliases: []string{"cd"},
	Commands: []*cli.Command{
		cmdChunkDeduplicateShowFiles,
		cmdToolChunkDeduplicateShowChunks,
	},
}

var cmdToolChunkDeduplicateShowChunks = &cli.Command{
	Name: "show-chunks",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "dsn",
			Aliases: []string{"d"},
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		dsn := command.String("dsn")
		db, err := pebble.Open(dsn, &pebble.Options{})
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		iter, err := db.NewIter(&pebble.IterOptions{
			LowerBound: prefixFileChunk,
			UpperBound: prefixFileChunkEnd,
		})
		if err != nil {
			return err
		}
		defer func() { _ = iter.Close() }()

		table := tablewriter.NewWriter(os.Stdout)
		table.Header("FileID", "Offset", "Length", "Hash")

		for iter.First(); iter.Valid(); iter.Next() {
			if !bytes.HasPrefix(iter.Key(), prefixFileChunk) {
				panic(errors.Newf("unexpected key: %x", iter.Key()))
			}

			key := iter.Key()
			assert.True(len(key) == 2+8*7)
			key = key[2:]
			hash := key[0:32]
			fileID := binary.BigEndian.Uint64(key[32:40])
			offset := binary.BigEndian.Uint64(key[40:48])
			length := binary.BigEndian.Uint64(key[48:56])

			err = table.Append(fileID, offset, length, fmt.Sprintf("0x%x", hash))
			if err != nil {
				panic(err)
			}
		}

		return table.Render()
	},
}

var cmdChunkDeduplicateShowFiles = &cli.Command{
	Name: "show-files",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "dsn",
			Aliases: []string{"d"},
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		dsn := command.String("dsn")
		db, err := pebble.Open(dsn, &pebble.Options{})
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		iter, err := db.NewIter(&pebble.IterOptions{
			LowerBound: prefixFileEntry,
			UpperBound: prefixFileEntryEnd,
		})
		if err != nil {
			return err
		}
		defer func() { _ = iter.Close() }()

		table := tablewriter.NewWriter(os.Stdout)
		table.Header("ID", "Path", "Hash")

		var fileEntry pb.FileEntry
		for iter.First(); iter.Valid(); iter.Next() {
			if !bytes.HasPrefix(iter.Key(), prefixFileEntry) {
				panic(errors.Newf("unexpected key: %x", iter.Key()))
			}

			value, err := iter.ValueAndErr()
			if err != nil {
				return err
			}

			err = proto.Unmarshal(value, &fileEntry)
			if err != nil {
				return errors.Wrapf(err, "key: %s", iter.Key())
			}

			err = table.Append(fileEntry.Id, fileEntry.Path, fmt.Sprintf("0x%x", fileEntry.Hash))
			if err != nil {
				return err
			}
		}

		err = iter.Error()
		if err != nil {
			return err
		}

		return table.Render()
	},
}
