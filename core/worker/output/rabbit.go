package output

import (
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"logical/config"
	"logical/core/model"
	"sync"
)

type rabbitOutput struct {
	cfg     *config.RabbitConfig
	pool    sync.Pool
	session struct {
		*amqp.Connection
		*amqp.Channel
	}
}

func NewRabbitOutput(cfg config.RabbitConfig) Output {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		zap.L().Error("create rabbitmq output error", zap.Error(err), zap.String("hosts", cfg.URL))
		return nil
	}
	o := rabbitOutput{cfg: &cfg}
	o.session.Connection = conn
	if o.session.Channel, err = conn.Channel(); err != nil {
		o.Close()
		return nil
	}
	// the queue is created automatically if it does not exist
	if _, err = o.session.Channel.QueueDeclare(
		cfg.Queue, true, false, false, false, nil); err != nil {
		zap.L().Error("declare exchange error", zap.Error(err), zap.String("queue", cfg.Queue))
		return nil
	}
	o.pool = sync.Pool{New: func() interface{} {
		return amqp.Publishing{
			ContentEncoding: "utf-8",
			ContentType:     "application/json",
			DeliveryMode:    amqp.Persistent,
			Body:            nil,
		}
	}}
	return &o
}

func (o *rabbitOutput) Write(records []*model.WalData) error {
	for i := 0; i < len(records); i++ {
		if err := o.publish(records[i]); err != nil {
			zap.L().Error("rabbit output error", zap.Error(err), zap.Any("data", records[i].Data))
		}
	}
	return nil
}

func (o *rabbitOutput) publish(data *model.WalData) error {
	zap.L().Debug("write to rabbit", zap.String("table", data.Table), zap.Any("data", data.Data))
	p := o.pool.Get().(amqp.Publishing)
	p.Body, _ = json.Marshal(data.Data)
	err := o.session.Channel.Publish("", o.cfg.Queue, false, false, p)
	o.pool.Put(p)
	return err
}

func (o *rabbitOutput) Close() {
	if o.session.Connection != nil {
		_ = o.session.Connection.Close()
	}
}
