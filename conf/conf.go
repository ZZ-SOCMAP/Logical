package conf

type Config struct {
	Subscribe SubscribeConfig `yaml:"subscribe"`
}

type SubscribeConfig struct {
	DumpPath   string   `yaml:"dump_path"`
	Historical string   `yaml:"historical"`
	DbHost     string   `yaml:"db_host"`
	DbPort     uint16   `yaml:"db_port"`
	DbName     string   `yaml:"db_name"`
	DbUser     string   `yaml:"db_user"`
	DbPass     string   `yaml:"db_pass"`
	DbKey      string   `yaml:"db_key"`
	SlotName   string   `yaml:"slot_name"`
	Tables     []string `yaml:"tables"`
}

// Conf ...
type Conf struct {
	// PgDumpExec pg_dump 可执行文件路径
	PgDumpExec string `json:"pg_dump_path"`
	// Subscribes 订阅规则
	Subscribes []*Subscribe `json:"subscribes"`
}

// PGConnConf of pg
type PGConnConf struct {
	// Host 地址
	Host string `json:"host"`
	// Port 端口
	Port uint16 `json:"port"`
	// Database database
	Database string `json:"database"`
	// Schema schema
	Schema string `json:"schema"`
	// User user
	User string `json:"user"`
	// Password password
	Password string `json:"password"`
}

// Subscribe 订阅一个数据库中的表的wal，根据规则保存到es里相应的index，type中
type Subscribe struct {
	// Dump 创建复制槽成功后，是否 dump 历史数据
	Dump bool `json:"dump"`
	// SlotName 逻辑复制槽
	SlotName string `json:"slotName"`
	// PGConnConf pg 连接配置
	PGConnConf *PGConnConf `json:"pgConnConf"`
	// Rules 订阅规则
	Rules []string `json:"rules"`
	// Retry 重试次数 -1:无限重试
	Retry int `json:"retry"`
}
