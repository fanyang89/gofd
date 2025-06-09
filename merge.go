package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
	"go.uber.org/zap"
)

var trimSpaceConfig = cli.StringConfig{TrimSpace: true}

func checkPath(p string) (s string, err error) {
	_, err = os.Stat(p)
	if err != nil {
		return
	}
	s, err = filepath.Abs(p)
	return
}

func mergePath(fs afero.Fs, dstPath string, srcPath string, dryRun bool) error {
	return afero.Walk(fs, srcPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		fileName := filepath.Base(path)
		fileDir := filepath.Dir(path)
		dstDir := filepath.Join(dstPath, strings.TrimPrefix(fileDir, srcPath))

		if dryRun {
			fmt.Println(fmt.Sprintf("[Dry run] Move file %s from %s to %s", fileName, fileDir, dstDir))
			return nil
		}

		_, err = fs.Stat(dstDir)
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
			err = fs.MkdirAll(dstDir, 0755)
		}

		action := MoveAction{fs: fs, dst: dstDir}
		err = action.Execute(path)
		if err != nil {
			if !errors.Is(err, ErrFileExists) {
				return err
			}
			var ok bool
			ok, err = fileHashEqual(path, filepath.Join(dstDir, fileName))
			if ok {
				return os.Remove(path)
			} else {
				zap.L().Info("Files are not the same", zap.String("file", fileName),
					zap.String("src", fileDir), zap.String("dst", dstDir))
			}
		}
		return nil
	})
}

func fileHashEqual(path1, path2 string) (ok bool, err error) {
	hash1, err := xxHashFile(path1)
	if err != nil {
		return
	}
	hash2, err := xxHashFile(path2)
	if err != nil {
		return
	}
	ok = hash1 == hash2
	return
}

var cmdMerge = &cli.Command{
	Name:  "merge",
	Usage: "Merge two directories",
	Arguments: []cli.Argument{
		&cli.StringArgs{Name: "path", Config: trimSpaceConfig, Max: 2},
	},
	Flags: []cli.Flag{
		&cli.BoolFlag{Name: "execute", Aliases: []string{"x"}},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		pathList := command.StringArgs("path")
		if len(pathList) != 2 {
			return errors.New("invalid path")
		}

		dstPath, err := checkPath(pathList[0])
		if err != nil {
			return err
		}

		srcPath, err := checkPath(pathList[1])
		if err != nil {
			return err
		}

		fs := afero.NewOsFs()
		return mergePath(fs, dstPath, srcPath, !command.Bool("execute"))
	},
}
