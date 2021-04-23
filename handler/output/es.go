package output

import (
	"bytes"
	"fmt"
	"logical/log"
	"strings"

	"github.com/olivere/elastic"
	"logical/conf"
	"logical/model"
)

type esHandler struct {
	client *elastic.Client
	sub    *conf.Subscribe
}

// newESOutput create handler write data to es
func newESOutput(sub *conf.Subscribe) Output {
	if sub.ESConf == nil {
		panic("es conf is nil")
	}

	client, err := elastic.NewSimpleClient(elastic.SetURL(strings.Split(sub.ESConf.Addrs, ",")...), elastic.SetBasicAuth(sub.ESConf.User, sub.ESConf.Password))
	if err != nil {
		panic(err)
	}

	handler := &esHandler{
		client: client,
		sub:    sub,
	}

	return handler
}

func (e *esHandler) Write(records ...*model.WalData) error {
	numReqs := len(records)
	if numReqs == 0 {
		return nil
	}

	log.Logger.Infof("Table: %s, Operate: %s, ID: %v", records[0].Table, records[0].OperationType, records[0].Data["id"])
	//var reqs = make([]elastic.BulkableRequest, 0, numReqs)
	//for _, data := range records {
	//	if req := e.makeRequest(data); req != nil {
	//		reqs = append(reqs, req)
	//	}
	//}
	//
	//if len(reqs) == 0 {
	//	return nil
	//}
	//
	//bulk := e.client.Bulk().Add(reqs...).Refresh("true")
	//
	//return util.WithRetry(e.sub.Retry, func() error {
	//	if _, err := bulk.Do(context.Background()); err != nil {
	//		log.Logger.Errorf("write es bulk request err: %v", err)
	//		return err
	//	}
	//	return nil
	//})
	return nil
}

func (e *esHandler) Close() {
	e.client.Stop()
}

func (e *esHandler) makeRequest(data *model.WalData) elastic.BulkableRequest {
	defer model.PutWalData(data)

	matchedRule := data.Rule

	var idBuf bytes.Buffer

	for _, field := range matchedRule.ESID {
		idBuf.WriteString(fmt.Sprint(data.Data[field]))
	}

	id := idBuf.String()
	if id == "" {
		return nil
	}

	switch data.OperationType {
	case model.Insert, model.Update:
		return elastic.NewBulkIndexRequest().
			Index(matchedRule.Index).
			Type(matchedRule.Type).
			Id(id).
			Doc(data.Data)
	case model.Delete:
		return elastic.NewBulkDeleteRequest().
			Index(matchedRule.Index).
			Type(matchedRule.Type).
			Id(id)
	}
	return nil
}
