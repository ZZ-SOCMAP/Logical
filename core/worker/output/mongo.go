package output

import (
	"fmt"
	"logical/config"
	"logical/core/model"
)

type MongoOutput struct {
	Cfg *config.MongoConfig
}

func (o *MongoOutput) Write(records []*model.WalData) error {
	fmt.Println("write mongo::", records)
	return nil
}

func (o *MongoOutput) Close() {

}
