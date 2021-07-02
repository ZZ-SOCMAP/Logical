package core

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	"go.uber.org/zap"
	"logical/core/engine"
	"sync"

	"logical/config"
)

type logical struct {
	cfg    *config.Config
	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup
}

func New(cfg *config.Config) *logical {
	return &logical{cfg: cfg}
}

func (l *logical) Start() error {
	// test database availability
	var dbCfg = pgx.ConnConfig{
		Host:     l.cfg.Capture.Database.Host,
		Port:     l.cfg.Capture.Database.Port,
		Database: l.cfg.Capture.Database.Name,
		User:     l.cfg.Capture.Database.Username,
		Password: l.cfg.Capture.Database.Password,
	}
	conn, err := pgx.ReplicationConnect(dbCfg)
	if err != nil {
		return fmt.Errorf("connect database: %s", err)
	}
	_ = conn.Close()

	// start core program
	l.wg = new(sync.WaitGroup)
	l.ctx, l.cancel = context.WithCancel(context.Background())
	for i := 0; i < len(l.cfg.Capture.Tables); i++ {
		var table = l.cfg.Capture.Tables[i]
		if l.cfg.Capture.Tables[i].Outputs == nil {
			zap.L().Warn("empty output, skipped", zap.String("table", table.Name))
			continue
		}
		l.wg.Add(1)
		s := engine.New(dbCfg, &table, &l.cfg.Output)
		go func() {
			defer l.wg.Done()
			if err = s.Start(l.ctx, table.Name, l.cfg.Capture.DumpPath, l.cfg.Capture.Historical); err != nil {
				zap.L().Error("start engine error", zap.Error(err), zap.String("table", table.Name))
			}
		}()
	}
	return nil
}
