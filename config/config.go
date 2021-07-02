package config

import (
	"github.com/BurntSushi/toml"
	"log"
	"sync"
)

type Config struct {
	Capture CaptureConfig `toml:"capture"`
	Output  OutputConfig  `toml:"output"`
	Logger  LoggerConfig  `toml:"logger"`
}

var once sync.Once

const TimeLayout = "2006-01-02 15:04:05.000000"

func Loading(cfgfile string) *Config {
	var cfg Config
	once.Do(func() {
		if _, err := toml.DecodeFile(cfgfile, &cfg); err != nil {
			log.Panicf("load config file error: %s", err)
		}
		for i := 0; i < len(cfg.Capture.Tables); i++ {
			if cfg.Capture.Tables[i].Fields != nil {
				cfg.Capture.Tables[i].Rule = make(map[string]struct{}, len(cfg.Capture.Tables[i].Fields))
				for f := 0; f < len(cfg.Capture.Tables[i].Fields); f++ {
					cfg.Capture.Tables[i].Rule[cfg.Capture.Tables[i].Fields[f]] = struct{}{}
				}
			}
			if cfg.Capture.Tables[i].Rule == nil {
				cfg.Capture.Tables[i].Rule = make(map[string]struct{}, 1)
			}
			if _, ok := cfg.Capture.Tables[i].Rule[cfg.Capture.Tables[i].Pk]; !ok {
				cfg.Capture.Tables[i].Rule[cfg.Capture.Tables[i].Pk] = struct{}{}
			}
		}
	})
	return &cfg
}

type OutputConfig struct {
	Kafka         KafkaConfig         `toml:"kafka"`
	ElasticSearch ElasticSearchConfig `toml:"elasticsearch"`
	RabbitMQ      RabbitConfig        `toml:"rabbitmq"`
	RestApi       RestApiConfig       `toml:"restapi"`
	MongoDB       MongoConfig         `toml:"mongo"`
}

type LoggerConfig struct {
	MaxAge  int    `toml:"maxage"`
	MaxSize int    `toml:"maxsize"`
	Backup  int    `toml:"backup"`
	Level   string `toml:"level"`
	Path    string `toml:"path"`
}

type CaptureConfig struct {
	DumpPath   string                `toml:"dump_path"`
	Historical bool                  `toml:"historical"`
	Database   CaptureDatabaseConfig `toml:"database"`
	Tables     []CaptureTableConfig  `toml:"tables"`
}

type CaptureTableConfig struct {
	Name     string   `toml:"name"`
	SlotName string   `toml:"slotname"`
	Fields   []string `toml:"fields"`
	Pk       string   `toml:"pk"`
	Outputs  []string `toml:"outputs"`
	Rule     map[string]struct{}
}

type CaptureDatabaseConfig struct {
	Host     string `toml:"host"`
	Port     uint16 `toml:"port"`
	Name     string `toml:"name"`
	Username string `toml:"username"`
	Password string `toml:"password"`
}

type KafkaConfig struct {
	Topic string   `toml:"topic"`
	Hosts []string `toml:"hosts"`
}

type ElasticSearchConfig struct {
	Index    string   `toml:"index"`
	Hosts    []string `toml:"hosts"`
	Username string   `toml:"username"`
	Password string   `toml:"password"`
}

type RabbitConfig struct {
	URL   string `toml:"url"`
	Queue string `toml:"queue"`
}

type RestApiConfig struct {
	URL string `toml:"url"`
}

type MongoConfig struct {
	URL string `toml:"url"`
}
