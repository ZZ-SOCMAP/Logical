package output

import (
	jsoniter "github.com/json-iterator/go"
	"logical/core/model"
)

type Output interface {
	Write(records []*model.WalData) error
	Close()
}

var json = jsoniter.ConfigCompatibleWithStandardLibrary
