package main

import (
	"context"
	"fmt"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"logical/config"
	"logical/core"
	"logical/core/handler"
	"logical/logger"
	"logical/proto"
	"os"
	"time"
)

var cfg config.Config

func loadConfig(cfgfile string) error {
	var file, err = ioutil.ReadFile(cfgfile)
	if err != nil {
		return fmt.Errorf("invalid config file path: %v", cfgfile)
	}
	if err = yaml.Unmarshal(file, &cfg); err != nil {
		return fmt.Errorf("invalid yaml configuration file: %v, Error: %v", cfgfile, err)
	}
	return nil
}

func pingUpstream(upstream *config.Upstream) error {
	var conn, err = handler.GetRpcConnect(upstream.Host)
	if err != nil {
		return err
	}
	var client = proto.NewLogicalHandlerClient(conn)
	var ctx, cancel = context.WithTimeout(context.Background(), time.Duration(upstream.Timeout)*time.Second)
	defer func() {
		cancel()
		ctx.Done()
		_ = conn.Close()
	}()
	reply, err := client.Ping(ctx, &proto.PingMessage{})
	if err != nil || reply.Status != true {
		return fmt.Errorf("some upstream health checks have failed: %s", upstream)
	}
	return nil
}

func startLogical(c *cli.Context) (err error) {
	if err = loadConfig(c.String("config")); err != nil {
		return err
	}
	if cfg.Logger.Level == "debug" {
		err = logger.InitDebugLogger(&cfg.Logger)
	} else {
		err = logger.InitReleaseLogger(&cfg.Logger)
	}
	if err != nil {
		return fmt.Errorf("failed to initialize the logging system")
	}
	if err = pingUpstream(&cfg.Upstream); err != nil {
		return err
	}
	var logical = core.New(&cfg)
	if err = logical.Start(); err != nil {
		return err
	}
	select {}
}

func main() {
	var cfgfile = &cli.StringFlag{
		Name:     "config",
		Value:    "./config.yaml",
		Aliases:  []string{"c"},
		Required: true,
		Usage:    "配置文件路径",
	}

	var app = &cli.App{
		Name:   "logical",
		Usage:  "Tool for synchronizing from PostgreSQL to custom handler through replication slot",
		Flags:  []cli.Flag{cfgfile},
		Action: startLogical,
	}

	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}
