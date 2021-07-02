package engine

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"logical/core/model"
	"logical/core/worker"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx"
	"logical/config"
)

// program control engine
type engine struct {
	cfg             pgx.ConnConfig
	tableCfg        *config.CaptureTableConfig
	connection      *pgx.ReplicationConn
	scheduler       *worker.Scheduler
	cancel          context.CancelFunc
	mutex           sync.Mutex
	receivePosition uint64
	replyPosition   uint64
	records         []*model.WalData
}

func New(cfg pgx.ConnConfig, table *config.CaptureTableConfig, output *config.OutputConfig) *engine {
	var e = &engine{
		cfg:      cfg,
		tableCfg: table,
	}
	e.scheduler = worker.StartScheduler(table.Outputs, output, e.setReplyPosition)
	return e
}

// getReceivePosition get receive position
func (e *engine) getReceivePosition() uint64 {
	return atomic.LoadUint64(&e.receivePosition)
}

// setReceivePosition set receive position
func (e *engine) setReceivePosition(position uint64) {
	atomic.StoreUint64(&e.receivePosition, position)
}

// getReplyPosition get reply position
func (e *engine) getReplyPosition() uint64 {
	return atomic.LoadUint64(&e.replyPosition)
}

// setReplyPosition set reply position
func (e *engine) setReplyPosition(position uint64) {
	atomic.StoreUint64(&e.replyPosition, position)
}

// get replication standard status
func (e *engine) status() (status *pgx.StandbyStatus, err error) {
	replyPosition := e.getReplyPosition()
	return pgx.NewStandbyStatus(e.getReceivePosition(), replyPosition, replyPosition)
}

// Start engine
func (e *engine) Start(ctx context.Context, table, dump string, historical bool) error {
	zap.L().Info(
		"start engine...",
		zap.String("slot", e.tableCfg.SlotName),
		zap.String("table", table),
		zap.Strings("output", e.tableCfg.Outputs),
	)
	ctx, e.cancel = context.WithCancel(ctx)
	ssid, err := e.connect()
	if err != nil {
		return err
	}
	if err = e.heartbeat(); err != nil {
		return err
	}
	if historical && ssid != "" {
		c := worker.NewSerializer(e.cfg, table, dump)
		if c != nil {
			if err = c.Listen(ssid, e.scheduler.Commit); err != nil {
				zap.L().Error("export snapshot error", zap.String("slot", e.tableCfg.SlotName), zap.Error(err))
			}
		}
	}
	return e.run(ctx)
}

// run loop
func (e *engine) run(ctx context.Context) error {
	defer func() { _ = e.stop() }()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				_ = e.heartbeat()
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		message, err := e.connection.WaitForReplicationMessage(ctx)
		if err != nil {
			if err == ctx.Err() {
				return err
			}
			if e.connection == nil || e.connection.IsAlive() {
				if _, err = e.connect(); err != nil {
					zap.L().Error("reset replication connection err", zap.Error(err))
				}
			}
			continue
		}
		if message == nil {
			continue
		}
		if err = e.replication(message); err != nil {
			zap.L().Error("handle replication message err", zap.Error(err))
			continue
		}
	}
}

// create replication connect
func (e *engine) connect() (ssid string, err error) {
	if e.connection, err = pgx.ReplicationConnect(e.cfg); err != nil {
		return ssid, err
	}
	if _, ssid, err = e.connection.CreateReplicationSlotEx(
		e.tableCfg.SlotName, "test_decoding"); err != nil {
		if pgerr, ok := err.(pgx.PgError); !ok || pgerr.Code != "42710" {
			zap.L().Debug("create replication slot error", zap.Error(err))
			return ssid, fmt.Errorf("failed to create replication slot: %s", err)
		}
	}
	if err = e.connection.StartReplication(e.tableCfg.SlotName, 0, -1); err != nil {
		_ = e.connection.Close()
		return ssid, err
	}
	return ssid, nil
}

// replication message
func (e *engine) replication(message *pgx.ReplicationMessage) (err error) {
	if message.ServerHeartbeat != nil {
		if message.ServerHeartbeat.ServerWalEnd > e.getReceivePosition() {
			e.setReceivePosition(message.ServerHeartbeat.ServerWalEnd)
		}
		if message.ServerHeartbeat.ReplyRequested == 1 {
			_ = e.heartbeat()
		}
	}
	if message.WalMessage != nil {
		var data = model.NewData()
		if err = data.Decode(message.WalMessage, e.tableCfg); err != nil {
			return fmt.Errorf("invalid postgres output message: %s", err)
		}
		if data.Timestamp > 0 {
			e.commit(data)
		}
	}
	return nil
}

// message commit handler
func (e *engine) commit(data *model.WalData) {
	var flush bool
	switch data.OperationType {
	case model.BEGIN, model.UNKNOW:
	case model.COMMIT:
		flush = true
	default:
		e.records = append(e.records, data)
		flush = len(e.records) > 1000
	}
	if flush && len(e.records) > 0 {
		e.scheduler.Commit(e.records)
		e.records = nil
	}
}

// send heartbeat to postgres
func (e *engine) heartbeat() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	status, err := e.status()
	if err != nil {
		return err
	}
	return e.connection.SendStandbyStatus(status)
}

// stop self
func (e *engine) stop() error {
	e.cancel()
	e.scheduler.Stop()
	return e.connection.Close()
}
