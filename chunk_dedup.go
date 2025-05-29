package main

import (
	"context"
	"database/sql"
	_ "embed"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/jotfs/fastcdc-go"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/urfave/cli/v3"
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
		db, err := sql.Open("duckdb", dsn)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		cd := NewChunkDeduplicator(db)
		err = cd.createTable()
		if err != nil {
			return err
		}

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
	db *sql.DB
}

func NewChunkDeduplicator(db *sql.DB) *ChunkDeduplicator {
	return &ChunkDeduplicator{
		db: db,
	}
}

//go:embed create_table.sql
var createTableSQL string

func (d *ChunkDeduplicator) createTable() error {
	_, err := d.db.Exec(createTableSQL)
	return err
}

var cdcOption = fastcdc.Options{
	MinSize:     4 * 1024,
	AverageSize: 1 * 1024 * 1024,
	MaxSize:     4 * 1024 * 1024,
}

func splitCDC(r io.Reader) func(yield func(chunk *fastcdc.Chunk) bool) {
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

func (d *ChunkDeduplicator) createFileRecord2(path string) (int64, error) {
	hash, err := getFileHash(path)
	if err != nil {
		return 0, err
	}

	r, err := d.db.Exec(`INSERT INTO files (path, hash) VALUES (?, ?)`, hash, path)
	if err != nil {
		return 0, err
	}

	// TODO: test this
	id, err := r.LastInsertId()
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (d *ChunkDeduplicator) createFileRecord(path string) (id int64, err error) {
	err = d.db.QueryRow(`SELECT id FROM files WHERE path = ?`, path).Scan(&id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			id, err = d.createFileRecord2(path)
		}
	}
	return
}

func (d *ChunkDeduplicator) ProcessFile(path string) error {
	fileID, err := d.createFileRecord(path)
	if err != nil {
		return err
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	for c := range splitCDC(f) {
		hash := getBufferHash(c.Data)
		_, err = d.db.Exec(`INSERT INTO file_chunks VALUES (?, ?, ?, ?)`,
			fileID, c.Offset, c.Length, hash)
		if err != nil {
			return err
		}
	}

	return nil
}
