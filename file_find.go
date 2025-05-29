package main

import (
	"context"
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
	"github.com/urfave/cli/v3"
	"go.uber.org/zap"
)

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
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		path := command.StringArg("path")
		action := newAction(command.String("action"))
		searchMode := newSearchType(command.String("type"))

		globStr := command.String("glob")
		matcher, err := glob.Compile(globStr)
		if err != nil {
			return err
		}

		pathList := make([]string, 0)

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

			if matcher.Match(path) {
				pathList = append(pathList, path)
			}

			return err
		})
		if err != nil {
			return err
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

type MoveAction struct {
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

func (a MoveAction) Execute(path string) error {
	fileName := filepath.Base(path)
	dstPath := filepath.Join(a.dst, fileName)

	zap.L().Info("Move to", zap.String("file", fileName), zap.String("dst", a.dst))
	_, err := os.Stat(dstPath)
	if err == nil {
		return errors.Newf("File already exists: %s", dstPath)
	}

	err = os.Rename(path, dstPath)
	if err != nil {
		if IsCrossDeviceLinkErrno(err) {
			var r, d *os.File
			r, err = os.Open(path)
			if err != nil {
				return err
			}

			d, err = os.Create(dstPath)
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

			return os.Remove(path)
		} else {
			return err
		}
	}
	return nil
}

func newAction(action string) Action {
	if action == "" {
		return OmitAction{}
	}

	const moveToPrefix = "move-to:"
	if strings.HasPrefix(action, moveToPrefix) {
		dst := strings.TrimPrefix(action, moveToPrefix)
		_, err := os.Stat(dst)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				_ = os.MkdirAll(dst, 0755)
			} else {
				panic(err)
			}
		}
		return MoveAction{dst: dst}
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
