package output

import (
	"fmt"
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"logical/config"
	"logical/core/model"
	"time"
)

type kafkaOutput struct {
	cfg      *config.KafkaConfig
	producer sarama.SyncProducer
}

func NewKafkaOutput(cfg config.KafkaConfig) Output {
	sarama.Logger = zap.NewStdLog(zap.L())
	pc := sarama.NewConfig()
	pc.Producer.Compression = sarama.CompressionSnappy  // Compress messages
	pc.Producer.Flush.Frequency = 10 * time.Millisecond // Flush batches every 10ms
	pc.Producer.Flush.MaxMessages = 1 << 29
	pc.Producer.RequiredAcks = sarama.NoResponse
	pc.Producer.Return.Successes = true
	pc.Producer.Retry.Max = 3
	pc.Producer.Partitioner = sarama.NewHashPartitioner
	producer, err := sarama.NewSyncProducer(cfg.Hosts, pc)
	if err != nil {
		return nil
	}
	return &kafkaOutput{producer: producer, cfg: &cfg}
}

func (o *kafkaOutput) Write(records []*model.WalData) error {
	var messages = make([]*sarama.ProducerMessage, 0, len(records))
	for i := 0; i < len(records); i++ {
		if message := o.build(records[i]); message != nil {
			messages = append(messages, message)
		}
	}
	return o.producer.SendMessages(messages)
}

func (o *kafkaOutput) build(data *model.WalData) *sarama.ProducerMessage {
	zap.L().Debug("write to kafka", zap.String("table", data.Table), zap.Any("data", data.Data))
	data.Data["operate"] = data.OperationType.String()
	message, _ := json.Marshal(data.Data)
	model.PutData(data)
	return &sarama.ProducerMessage{
		Topic: o.cfg.Topic,
		Key:   sarama.StringEncoder(fmt.Sprintf("%v", data.Data["pk"])),
		Value: sarama.ByteEncoder(message),
	}
}

func (o *kafkaOutput) Close() {
	_ = o.producer.Close()
}
