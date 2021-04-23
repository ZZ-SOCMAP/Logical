package conf

type Config struct {
	Subscribe SubscribeConfig `yaml:"subscribe"`
}

type SubscribeConfig struct {
	DumpPath   string   `yaml:"dump_path"`
	Historical bool     `yaml:"historical"`
	DbHost     string   `yaml:"db_host"`
	DbPort     uint16   `yaml:"db_port"`
	DbName     string   `yaml:"db_name"`
	DbUser     string   `yaml:"db_user"`
	DbPass     string   `yaml:"db_pass"`
	DbKey      string   `yaml:"db_key"`
	SlotName   string   `yaml:"slot_name"`
	Upstream   string   `yaml:"upstream"`
	Tables     []string `yaml:"tables"`
}
