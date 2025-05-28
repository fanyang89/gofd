package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gobwas/glob"
	"github.com/urfave/cli/v3"
	"go.uber.org/zap"
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
		action := newAction(command.String("action"))
		mode := newSearchType(command.String("type"))

		globStr := command.String("glob")
		matcher, err := glob.Compile(globStr)
		if err != nil {
			return err
		}

		return filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}

			switch mode {
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

func main() {
	logger, _ := zap.NewDevelopment(zap.AddCaller(), zap.AddCallerSkip(1))
	zap.ReplaceGlobals(logger)
	defer func() { _ = logger.Sync() }()

	err := cmd.Run(context.Background(), os.Args)
	if err != nil {
		zap.L().Error("Unexpected error", zap.Error(err))
	}
}
