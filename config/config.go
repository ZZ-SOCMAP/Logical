package config

// Config 配置项
type Config struct {
	Upstream Upstream `yaml:"upstream"`
	Capture  Capture  `yaml:"capture"`
	Logger   Logger   `yaml:"logger"`
}

// Capture 数据库捕获配置
type Capture struct {
	DumpPath   string   `yaml:"dump_path"`
	Historical bool     `yaml:"historical"`
	DbHost     string   `yaml:"db_host"`
	DbPort     uint16   `yaml:"db_port"`
	DbName     string   `yaml:"db_name"`
	DbUser     string   `yaml:"db_user"`
	DbPass     string   `yaml:"db_pass"`
	DbKey      string   `yaml:"db_key"`
	SlotName   string   `yaml:"slot_name"`
	Tables     []string `yaml:"tables"`
}

// Logger 日志配置
type Logger struct {
	Level     string `yaml:"level"`
	Maxsize   int    `yaml:"size"`
	MaxAge    int    `yaml:"age"`
	MaxBackup int    `yaml:"backup"`
	Savepath  string `yaml:"savepath"`
}

// Upstream 上游服务配置
type Upstream struct {
	Host    string `yaml:"host"`
	Timeout int    `yaml:"timeout"`
}
