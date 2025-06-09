package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/urfave/cli/v3"
)

var cmdStat = &cli.Command{
	Name: "stat",
	Arguments: []cli.Argument{
		&cli.StringArg{
			Name: "path",
		},
	},
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "output",
			Aliases: []string{"o"},
			Value:   "-",
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		path := command.StringArg("path")
		if path == "" {
			return errors.New("path is required")
		}

		var writer *bufio.Writer
		output := command.String("output")
		if output == "-" {
			writer = bufio.NewWriter(os.Stdout)
		} else {
			f, err := os.Create(output)
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()
			writer = bufio.NewWriter(f)
		}

		w := csv.NewWriter(writer)
		err := w.Write([]string{"name", "size"})
		if err != nil {
			return err
		}

		err = filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			s, err := d.Info()
			if err != nil {
				return err
			}
			return w.Write([]string{d.Name(), fmt.Sprintf("%d", s.Size())})
		})
		if err != nil {
			return err
		}

		w.Flush()
		return writer.Flush()
	},
}
