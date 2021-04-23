package output

import (
	"logical/conf"
	"logical/model"
)

// Output for write wal data
type Output interface {
	Write(wal ...*model.WalData) error
	Close()
}

// NewOutput create new output
func NewOutput(sub *conf.SubscribeConfig) Output {
	return newESOutput(sub)
}
