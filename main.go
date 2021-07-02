package main

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"log"
	"logical/config"
	"logical/core"
	"logical/logger"
	"os"
)

func startLogical(c *cli.Context) (err error) {
	var cfgfile = c.String("config")
	cfg := config.Loading(cfgfile)
	switch cfg.Logger.Level {
	case "debug":
		err = logger.InitDebugLogger(&cfg.Logger)
	default:
		err = logger.InitReleaseLogger(&cfg.Logger)
	}
	if err != nil {
		return fmt.Errorf("logger config error:%s", err.Error())
	}
	var logical = core.New(cfg)
	if err = logical.Start(); err == nil {
		select {}
	}
	return err
}

const (
	name    = "logical"
	version = "v0.1.0"
	usage   = "Tool for synchronizing from PostgreSQL to custom handler through replication slot"
)

func main() {
	var cfgfile = &cli.StringFlag{
		Name:     "config",
		Value:    "./config.toml",
		Aliases:  []string{"c"},
		Required: true,
		Usage:    "specify the config(toml) `filepath`",
	}

	var app = &cli.App{Name: name, Usage: usage, Version: version, Flags: []cli.Flag{cfgfile}, Action: startLogical}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
