package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/cockroachdb/errors"
	"github.com/gobwas/glob"
	"github.com/laurent22/go-trash"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
	"go.uber.org/zap"
)

type exclude struct {
	patterns []glob.Glob
}

func (e exclude) Match(path string) bool {
	if len(e.patterns) == 0 {
		return false
	}
	for _, pattern := range e.patterns {
		if pattern.Match(path) {
			return true
		}
	}
	return false
}

func newExclude(patterns []string) *exclude {
	p := make([]glob.Glob, len(patterns))
	for i, pattern := range patterns {
		g, err := glob.Compile(pattern)
		if err != nil {
			panic(err)
		}
		p[i] = g
	}
	return &exclude{p}
}

var cmdFind = &cli.Command{
	Name: "find",
	Arguments: []cli.Argument{
		&cli.StringArg{Name: "path"},
	},
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "glob",
			Aliases: []string{"g"},
			Value:   "*",
		},
		&cli.StringFlag{
			Name:    "action",
			Aliases: []string{"x"},
		},
		&cli.StringFlag{
			Name:    "type",
			Aliases: []string{"t"},
		},
		&cli.StringSliceFlag{
			Name:    "excludes",
			Aliases: []string{"e"},
		},
		&cli.StringFlag{
			Name: "base-dir",
		},
		&cli.StringFlag{
			Name: "dsn",
		},
		&cli.StringFlag{
			Name: "sql",
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		path := command.StringArg("path")
		action := newAction(command.String("action"))
		searchMode := newSearchType(command.String("type"))
		exclude := newExclude(command.StringSlice("excludes"))

		dsn := command.String("dsn")
		sqlStatement := command.String("sql")
		if (dsn == "" && sqlStatement != "") || (dsn != "" && sqlStatement == "") {
			return errors.New("dsn or sql statement both required")
		}

		globStr := command.String("glob")
		matcher, err := glob.Compile(globStr)
		if err != nil {
			return err
		}

		pathList := make([]string, 0)

		if dsn != "" {
			db, err := sql.Open("sqlite3", dsn)
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()

			rows, err := db.Query(sqlStatement)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()

			baseDir := command.String("base-dir")
			for rows.Next() {
				var p string
				err = rows.Scan(&p)
				if err != nil {
					return err
				}
				if baseDir != "" {
					p = filepath.Join(baseDir, p)
				}
				pathList = append(pathList, p)
			}
		} else {
			err = filepath.WalkDir(path, func(path string, info os.DirEntry, err error) error {
				if err != nil {
					return err
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

				if matcher.Match(path) && !exclude.Match(path) {
					pathList = append(pathList, path)
				}

				return err
			})
			if err != nil {
				return err
			}
		}

		for _, path := range pathList {
			err = action.Execute(path)
			if err != nil {
				zap.L().Error("Action execute failed", zap.String("path", path), zap.Error(err))
			}
		}

		return nil
	},
}

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
	if trash.IsAvailable() {
		p, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		_, err = trash.MoveToTrash(p)
		return err
	}
	return os.RemoveAll(path)
}

type CopyAction struct {
	dst string
}

func (a CopyAction) Execute(path string) error {
	fileName := filepath.Base(path)
	dstPath := filepath.Join(a.dst, fileName)

	zap.L().Info("Copy to", zap.String("file", fileName), zap.String("dst", a.dst))
	_, err := os.Stat(dstPath)
	if err == nil {
		return errors.Wrapf(ErrFileExists, "path: %s", dstPath)
	}

	var r, d *os.File
	r, err = os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()

	d, err = os.Create(dstPath)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()

	_, err = io.Copy(d, r)
	return err
}

type MoveAction struct {
	fs  afero.Fs
	dst string
}

func IsCrossDeviceLinkErrno(errno error) bool {
	if runtime.GOOS == "windows" {
		// 0x11 is Win32 Error Code ERROR_NOT_SAME_DEVICE
		// See: https://msdn.microsoft.com/en-us/library/cc231199.aspx
		return errors.Is(errno, syscall.Errno(0x11))
	}
	return errors.Is(errno, syscall.EXDEV)
}

var ErrFileExists = errors.New("file exists")

func (a MoveAction) Execute(path string) error {
	fileName := filepath.Base(path)
	dstPath := filepath.Join(a.dst, fileName)

	zap.L().Info("Move to", zap.String("file", fileName), zap.String("dst", a.dst))
	_, err := a.fs.Stat(dstPath)
	if err == nil {
		return errors.Wrapf(ErrFileExists, "path: %s", dstPath)
	}

	err = a.fs.Rename(path, dstPath)
	if err != nil {
		if IsCrossDeviceLinkErrno(err) {
			var r, d afero.File
			r, err = a.fs.Open(path)
			if err != nil {
				return err
			}

			d, err = a.fs.Create(dstPath)
			if err != nil {
				_ = r.Close()
				return err
			}

			_, err = io.Copy(d, r)
			_ = r.Close()
			_ = d.Close()
			if err != nil {
				return err
			}

			return a.fs.Remove(path)
		} else {
			return err
		}
	}
	return nil
}

func createDirectory(path string) {
	_, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			_ = os.MkdirAll(path, 0755)
		} else {
			panic(err)
		}
	}
}

func newAction(action string) Action {
	if action == "" {
		return OmitAction{}
	}

	const moveToPrefix = "move-to:"
	if strings.HasPrefix(action, moveToPrefix) {
		dst := strings.TrimPrefix(action, moveToPrefix)
		createDirectory(dst)
		return MoveAction{fs: afero.NewOsFs(), dst: dst}
	}

	const copyToPrefix = "copy-to:"
	if strings.HasPrefix(action, copyToPrefix) {
		dst := strings.TrimPrefix(action, copyToPrefix)
		createDirectory(dst)
		return CopyAction{dst: dst}
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
