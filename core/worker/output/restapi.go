package output

import (
	"fmt"
	"logical/config"
	"logical/core/model"
)

type RestapiOutput struct {
	Cfg *config.RestApiConfig
}

func (o *RestapiOutput) Write(records []*model.WalData) error {
	fmt.Println("write restapi::", records)
	return nil
}

func (o *RestapiOutput) Close() {

}
