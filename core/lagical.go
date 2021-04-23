package core

import (
	"context"
	"sync"

	"logical/conf"
	"logical/log"
)

// this is a static check
var _ Interface = (*river)(nil)

// Interface of river
type Interface interface {
	Start() error
	Stop()
	Update(config *conf.Config)
}

type river struct {
	conf   *conf.Config
	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup
}

// New create river from conf
func New(conf *conf.Config) Interface {
	return &river{conf: conf}
}

// Start flow the river
func (r *river) Start() error {
	r.wg = new(sync.WaitGroup)
	r.ctx, r.cancel = context.WithCancel(context.Background())
	if r.conf != nil {
		r.wg.Add(1)
		stream := newStream(r.conf)
		go func() { _ = stream.start(r.ctx, r.wg) }()
	}
	log.Logger.Info("start logical...")
	return nil
}

func (r *river) Update(config *conf.Config) {
	// stop running streams
	r.Stop()
	r.conf = config
	r.wg = new(sync.WaitGroup)
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.wg.Add(1)
	var stream = newStream(r.conf)
	go func() { _ = stream.start(r.ctx, r.wg) }()
}

func (r *river) Stop() {
	r.cancel()
	r.wg.Wait()
}
