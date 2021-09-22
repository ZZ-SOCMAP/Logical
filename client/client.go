package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx"
	"github.com/yanmengfei/logical/config"
	"github.com/yanmengfei/logical/model"
)

type client struct {
	connCfg         pgx.ConnConfig
	tableCfg        *config.TableConfig
	repConn         *pgx.ReplicationConn
	cancel          context.CancelFunc
	mutex           sync.Mutex
	receivePosition uint64
	replyPosition   uint64
	callback        func(records []*model.Waldata)
	records         []*model.Waldata
}

func New(dbCfg *config.DatabaseConfig, tbCfg *config.TableConfig, callback func(records []*model.Waldata)) (*client, error) {
	connCfg := pgx.ConnConfig{
		Host:     dbCfg.Host,
		Port:     dbCfg.Port,
		Database: dbCfg.DbName,
		User:     dbCfg.Username,
		Password: dbCfg.Password,
	}
	return &client{connCfg: connCfg, tableCfg: tbCfg, callback: callback}, nil
}

// getReceivePosition get receive position
func (c *client) getReceivePosition() uint64 {
	return atomic.LoadUint64(&c.receivePosition)
}

// setReceivePosition set receive position
func (c *client) setReceivePosition(position uint64) {
	atomic.StoreUint64(&c.receivePosition, position)
}

// getReplyPosition get reply position
func (c *client) getReplyPosition() uint64 {
	return atomic.LoadUint64(&c.replyPosition)
}

// setReplyPosition set reply position
func (c *client) setReplyPosition(position uint64) {
	atomic.StoreUint64(&c.replyPosition, position)
}

func (c *client) status() (status *pgx.StandbyStatus, err error) {
	replyPosition := c.getReplyPosition()
	return pgx.NewStandbyStatus(c.getReceivePosition(), replyPosition, replyPosition)
}

// send heartbeat to postgres
func (c *client) heartbeat() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	status, err := c.status()
	if err != nil {
		return err
	}
	return c.repConn.SendStandbyStatus(status)
}

// create replication connect
func (c *client) connect() (ssid string, err error) {
	if c.repConn, err = pgx.ReplicationConnect(c.connCfg); err != nil {
		return ssid, err
	}
	if _, ssid, err = c.repConn.CreateReplicationSlotEx(
		c.tableCfg.SlotName, "test_decoding"); err != nil {
		if pgerr, ok := err.(pgx.PgError); !ok || pgerr.Code != "42710" {
			return ssid, fmt.Errorf("failed to create replication slot: %s", err)
		}
	}
	if err = c.repConn.StartReplication(c.tableCfg.SlotName, 0, -1); err != nil {
		_ = c.repConn.Close()
		return ssid, err
	}
	return ssid, nil
}

// replication message
func (c *client) replication(message *pgx.ReplicationMessage) (err error) {
	if message.ServerHeartbeat != nil {
		if message.ServerHeartbeat.ServerWalEnd > c.getReceivePosition() {
			c.setReceivePosition(message.ServerHeartbeat.ServerWalEnd)
		}
		if message.ServerHeartbeat.ReplyRequested == 1 {
			_ = c.heartbeat()
		}
	}
	if message.WalMessage != nil {
		var data = model.AcquireWaldata()
		if err = data.Decode(message.WalMessage, c.tableCfg.Name); err != nil {
			return fmt.Errorf("invalid postgres output message: %s", err)
		}
		if data.Timestamp > 0 {
			c.commit(data)
		}
	}
	return nil
}

// message commit handler
func (c *client) commit(data *model.Waldata) {
	var flush bool
	switch data.OperationType {
	case model.BEGIN, model.UNKNOW:
	case model.COMMIT:
		flush = true
	default:
		c.records = append(c.records, data)
		flush = len(c.records) > 1000
	}
	if flush && len(c.records) > 0 {
		c.callback(c.records)
		c.records = nil
	}
}

func (c *client) timer(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			_ = c.heartbeat()
		case <-ctx.Done():
			return
		}
	}
}

func (c *client) Start(ctx context.Context) error {
	ctx, c.cancel = context.WithCancel(ctx)
	_, err := c.connect()
	if err != nil {
		return err
	}
	if err = c.heartbeat(); err != nil {
		return err
	}
	go c.timer(ctx)
	for {
		message, err := c.repConn.WaitForReplicationMessage(ctx)
		if err != nil {
			if err == ctx.Err() {
				return err
			}
			if c.repConn == nil || c.repConn.IsAlive() {
				if _, err = c.connect(); err != nil {
					return fmt.Errorf("reset replication connection error: %s", err)
				}
			}
			continue
		}
		if message == nil {
			continue
		}
		if err = c.replication(message); err != nil {
			continue
		}
	}
}

func (c *client) Stop() error {
	c.cancel()
	return c.repConn.Close()
}
