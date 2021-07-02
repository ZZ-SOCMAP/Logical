package config

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

const content = `[capture]
dump_path = "/usr/local/bin/pg_dump"
historical = true

[capture.database]
host = "localhost"
port = 5432
name = "test"
username = "test"
password = "test"

[[capture.tables]]
name = "test"
pk = "id"
slotname = "test"
outputs = ["rabbitmq", "kafka"]

[logger]
maxsize = 100
maxage = 7
backup = 10
level = "debug"
path = "logs/logical.log"

[output.kafka]
hosts = ["127.0.0.1:9092"]
topic = "test"

[output.rabbitmq]
url = "amqp://admin:admin@localhost:5672/vhost"
queue = "test"
`

func TestNewConfig(t *testing.T) {
	var expCfg = &Config{
		Capture: CaptureConfig{
			DumpPath:   "/usr/local/bin/pg_dump",
			Historical: true,
			Database:   CaptureDatabaseConfig{Name: "test", Port: 5432, Username: "test", Password: "test", Host: "localhost"},
			Tables:     []CaptureTableConfig{{Name: "test", Pk: "id", SlotName: "test", Outputs: []string{"rabbitmq", "kafka"}, Rule: map[string]struct{}{"id": {}}}},
		},
		Logger: LoggerConfig{MaxAge: 7, MaxSize: 100, Backup: 10, Level: "debug", Path: "logs/logical.log"},
		Output: OutputConfig{
			Kafka:    KafkaConfig{Hosts: []string{"127.0.0.1:9092"}, Topic: "test"},
			RabbitMQ: RabbitConfig{URL: "amqp://admin:admin@localhost:5672/vhost", Queue: "test"},
		},
	}
	filename := "test.cache"
	_ = ioutil.WriteFile(filename, []byte(content), 0644)
	defer func() { _ = os.Remove(filename) }()
	actCfg := Loading(filename)
	if !reflect.DeepEqual(expCfg, actCfg) {
		t.Errorf("nconsistent data\n\nexpect: %v\nactual: %v\n", *expCfg, *actCfg)
	}
}
