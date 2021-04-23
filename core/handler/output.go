package handler

import (
	model2 "logical/core/model"
	"logical/log"
)

type Output struct {
	upstream string
}

// NewOutput create new output
func NewOutput(upstream string) Output {
	return Output{upstream: upstream}
}

func (o *Output) Write(records ...*model2.WalData) error {
	if len(records) == 0 {
		return nil
	}
	log.Logger.Infof("Table: %s, Operate: %s, ID: %v", records[0].Table, records[0].OperationType, records[0].Data["id"])
	return nil
}

func (o *Output) Close() {
}
