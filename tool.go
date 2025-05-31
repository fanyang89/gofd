package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/negrel/assert"
	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v3"
	"google.golang.org/protobuf/proto"

	"github.com/fanyang89/gofd/pb"
)

var cmdTool = &cli.Command{
	Name: "tool",
	Commands: []*cli.Command{
		cmdToolChunkDeduplicate,
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
