package config

type DatabaseConfig struct {
	Host     string
	Port     uint16
	DbName   string
	Username string
	Password string
}

type TableConfig struct {
	Name     string
	SlotName string
}
