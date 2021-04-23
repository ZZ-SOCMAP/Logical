package conf

// Conf ...
type Conf struct {
	// PgDumpExec pg_dump 可执行文件路径
	PgDumpExec string `json:"pg_dump_path"`
	// Subscribes 订阅规则
	Subscribes []*Subscribe `json:"subscribes"`
}

// ESConf es 配置
type ESConf struct {
	// Addrs es 地址
	Addrs string `json:"addrs"`
	// User es username
	User string `json:"user"`
	// Password es password
	Password string `json:"password"`
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
	Rules []*Rule `json:"rules"`
	// ESConf ES 配置
	ESConf *ESConf `json:"esConf"`
	// Retry 重试次数 -1:无限重试
	Retry int `json:"retry"`
}

// Rule 同步规则
type Rule struct {
	// Table 订阅数据表，支持 ?* 通配符匹配
	Table string `json:"table"`
	// 下面几项同步到es中时需配置
	// ESID 用作es中id的字段，多个字段内容会连在一起
	ESID []string `json:"esid"`
	// Index es中的idex
	Index string `json:"index"`
	// Type es中的type
	Type string `json:"type"`

	// 下面几项同步到kafka中时需要配置
	// Topic ...
	Topic string `json:"topic"`
}
