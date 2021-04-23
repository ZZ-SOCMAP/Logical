package output

import (
	"logical/conf"
	"logical/log"
	"logical/model"
)

type esHandler struct {
	sub *conf.Subscribe
}

// newESOutput create handler write data to es
func newESOutput(sub *conf.Subscribe) Output {
	return &esHandler{sub: sub}
}

func (e *esHandler) Write(records ...*model.WalData) error {
	if len(records) == 0 {
		return nil
	}
	log.Logger.Infof("Table: %s, Operate: %s, ID: %v", records[0].Table, records[0].OperationType, records[0].Data["id"])
	return nil
}

func (e *esHandler) Close() {
}
