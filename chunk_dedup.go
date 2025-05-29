package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"log"

	"github.com/jotfs/fastcdc-go"
	"github.com/urfave/cli/v3"
)

var cmdDeduplicateChunk = &cli.Command{
	Name: "chunk",
}

type ChunkDeduplicator struct {
}

func NewChunkDeduplicator() *ChunkDeduplicator {
	return &ChunkDeduplicator{}
}

func (d *ChunkDeduplicator) Deduplicate() error {
	opts := fastcdc.Options{
		MinSize:     4 * 1024,
		AverageSize: 1 * 1024 * 1024,
		MaxSize:     4 * 1024 * 1024,
	}

	data := make([]byte, 10*1024*1024)
	rand.Read(data)
	chunker, _ := fastcdc.NewChunker(bytes.NewReader(data), opts)

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("data: %x, len: %d\n", chunk.Data[:10], chunk.Length)
	}

	return nil
}
