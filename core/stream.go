package core

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	dump2 "logical/core/dump"
	handler2 "logical/core/handler"
	model2 "logical/core/model"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx"
	"logical/config"
)

type stream struct {
	// 订阅配置
	cfg *config.Config
	// 当前 wal 位置
	receivedWal uint64
	flushWal    uint64
	// 复制连接
	replicationConn *pgx.ReplicationConn
	// 消息处理
	handler handler2.Handler
	// 取消
	cancel context.CancelFunc
	// ack 锁
	sendStatusLock sync.Mutex
	// buffered data
	records []*model2.WalData
}

func (s *stream) getReceivedWal() uint64 {
	return atomic.LoadUint64(&s.receivedWal)
}

func (s *stream) setReceivedWal(val uint64) {
	atomic.StoreUint64(&s.receivedWal, val)
}

func (s *stream) getFlushWal() uint64 {
	return atomic.LoadUint64(&s.flushWal)
}

func (s *stream) setFlushWal(val uint64) {
	atomic.StoreUint64(&s.flushWal, val)
}

func (s *stream) getStatus() (*pgx.StandbyStatus, error) {
	return pgx.NewStandbyStatus(s.getReceivedWal(), s.getFlushWal(), s.getFlushWal())
}

func newStream(cfg *config.Config) *stream {
	var ret = &stream{cfg: cfg}
	ret.handler = handler2.NewHandler(&cfg.Capture, &cfg.Upstream, ret.setFlushWal)
	return ret
}

func (s *stream) start(ctx context.Context, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	zap.L().Info("start stream...", zap.String("slot", s.cfg.Capture.SlotName))
	ctx, s.cancel = context.WithCancel(ctx)
	var cfg = pgx.ConnConfig{Host: s.cfg.Capture.DbHost, Port: s.cfg.Capture.DbPort, Database: s.cfg.Capture.DbName, User: s.cfg.Capture.DbUser, Password: s.cfg.Capture.DbPass}
	s.replicationConn, err = pgx.ReplicationConnect(cfg)
	if err != nil {
		zap.L().Error("create replication connection err", zap.Error(err))
		return err
	}
	_, snapshotid, err := s.replicationConn.CreateReplicationSlotEx(s.cfg.Capture.SlotName, "test_decoding")
	if err != nil {
		// 42710 means replication slot already exists
		if pgerr, ok := err.(pgx.PgError); !ok || pgerr.Code != "42710" {
			zap.L().Error("create replication slot err: %v", zap.Error(err))
			return fmt.Errorf("failed to create replication slot: %s", err)
		}
	}

	_ = s.heartbeat()
	// Handle old data from db
	if err = s.exportSnapshot(snapshotid); err != nil {
		zap.L().Error("export snapshot %s err: %v", zap.String("snapshotid", snapshotid), zap.Error(err))
		return fmt.Errorf("slot name %s, err export snapshot: %v", s.cfg.Capture.SlotName, err)
	}

	if err = s.replicationConn.StartReplication(s.cfg.Capture.SlotName, 0, -1); err != nil {
		zap.L().Error("start replication err", zap.Error(err))
		return err
	}

	return s.runloop(ctx)
}

func (s *stream) stop() error {
	s.cancel()
	s.handler.Stop()
	return s.replicationConn.Close()
}

func (s *stream) exportSnapshot(snapshotID string) error {
	// replication slot already exists
	if snapshotID == "" || !s.cfg.Capture.Historical {
		return nil
	}
	dumper := dump2.New(s.cfg.Capture.DumpPath, &s.cfg.Capture)
	return dumper.Dump(snapshotID, s.handler)
}

func (s *stream) runloop(ctx context.Context) error {
	defer func() { _ = s.stop() }()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				_ = s.heartbeat()
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		msg, err := s.replicationConn.WaitForReplicationMessage(ctx)
		if err != nil {
			if err == ctx.Err() {
				return err
			}
			if err = s.checkAndResetConn(); err != nil {
				zap.L().Error("reset replication connection err", zap.Error(err))
			}
			continue
		}

		if msg == nil {
			continue
		}

		if err = s.replicationMsgHandle(msg); err != nil {
			zap.L().Error("handle replication msg err", zap.Error(err))
			continue
		}
	}
}

func (s *stream) checkAndResetConn() error {
	if s.replicationConn != nil && s.replicationConn.IsAlive() {
		return nil
	}
	time.Sleep(time.Second * 10)
	var cfg = pgx.ConnConfig{
		Host:     s.cfg.Capture.DbHost,
		Port:     s.cfg.Capture.DbPort,
		Database: s.cfg.Capture.DbName,
		User:     s.cfg.Capture.DbUser,
		Password: s.cfg.Capture.DbPass,
	}
	conn, err := pgx.ReplicationConnect(cfg)
	if err != nil {
		return err
	}
	if _, _, err := conn.CreateReplicationSlotEx(s.cfg.Capture.SlotName, "test_decoding"); err != nil {
		if pgerr, ok := err.(pgx.PgError); !ok || pgerr.Code != "42710" {
			return fmt.Errorf("failed to create replication slot: %s", err)
		}
	}

	if err := conn.StartReplication(s.cfg.Capture.SlotName, 0, -1); err != nil {
		_ = conn.Close()
		return err
	}

	s.replicationConn = conn

	return nil
}

// ReplicationMsgHandle handle replication msg
func (s *stream) replicationMsgHandle(msg *pgx.ReplicationMessage) error {
	// 回复心跳
	if msg.ServerHeartbeat != nil {
		if msg.ServerHeartbeat.ServerWalEnd > s.getReceivedWal() {
			s.setReceivedWal(msg.ServerHeartbeat.ServerWalEnd)
		}
		if msg.ServerHeartbeat.ReplyRequested == 1 {
			_ = s.heartbeat()
		}
	}
	if msg.WalMessage != nil {
		logmsg, err := model2.Parse(msg.WalMessage)
		if err != nil {
			return fmt.Errorf("invalid pgoutput msg: %s", err)
		}

		logmsg.Timestamp = time.Now().UnixNano() / int64(time.Millisecond)
		if err := s.handleMessage(logmsg); err != nil {
			return err
		}
	}

	return nil
}

func (s *stream) handleMessage(data *model2.WalData) (err error) {
	var flush bool
	switch data.OperationType {
	// 事务开始
	case model2.Begin:
	// 	事务结束
	case model2.Commit:
		flush = true
	default:
		s.records = append(s.records, data)
		// 防止大事务耗尽内存
		flush = len(s.records) > 1000
	}

	if flush {
		_ = s.flush()
	}

	return nil
}

func (s *stream) flush() error {
	if len(s.records) > 0 {
		_ = s.handler.Handle(s.records...)
		s.records = nil
	}
	return nil
}

// heartbeat 发送心跳
func (s *stream) heartbeat() error {
	s.sendStatusLock.Lock()
	defer s.sendStatusLock.Unlock()
	zap.L().Debug("send heartbeat")
	status, err := s.getStatus()
	if err != nil {
		return err
	}
	return s.replicationConn.SendStandbyStatus(status)
}
