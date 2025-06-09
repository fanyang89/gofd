package main

import (
	"context"
	"fmt"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/urfave/cli/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var cmd = &cli.Command{
	Name:  "gofd",
	Usage: "fd in go",
	Commands: []*cli.Command{
		cmdFind,
		cmdDeduplicate,
		cmdTool,
		cmdStat,
		cmdMerge,
		cmdHash,
	},
}

var cmdDeduplicate = &cli.Command{
	Name:    "deduplicate",
	Aliases: []string{"dedup", "d"},
	Commands: []*cli.Command{
		cmdDeduplicateFile,
		cmdDeduplicateChunk,
	},
}

var cmdHash = &cli.Command{
	Name: "hash",
	Commands: []*cli.Command{
		cmdXXHash,
	},
}

var cmdXXHash = &cli.Command{
	Name:    "xxhash",
	Aliases: []string{"xxh"},
	Arguments: []cli.Argument{
		&cli.StringArg{Name: "path", Config: trimSpaceConfig},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		path := command.StringArg("path")
		if path == "" {
			return errors.New("path is required")
		}

		_, err := os.Stat(path)
		if err != nil {
			return err
		}

		h, err := xxHashFile(path)
		if err != nil {
			return err
		}
		fmt.Printf("0x%x\n", h)
		return nil
	},
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
