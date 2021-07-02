package output

import (
	"context"
	"github.com/olivere/elastic"
	"go.uber.org/zap"
	"logical/config"
	"logical/core/model"
)

type elasticOutput struct {
	cfg    *config.ElasticSearchConfig
	client *elastic.Client
}

func NewElasticOutput(cfg config.ElasticSearchConfig) Output {
	client, err := elastic.NewSimpleClient(
		elastic.SetURL(cfg.Hosts...),
		elastic.SetBasicAuth(cfg.Username, cfg.Password),
	)
	if err != nil {
		zap.L().Error("create elastic output failed", zap.Error(err), zap.Strings("hosts", cfg.Hosts))
		return nil
	}
	if _, _, err = client.Ping(cfg.Hosts[0]).Do(context.Background()); err != nil {
		return nil
	}
	return &elasticOutput{cfg: &cfg, client: client}
}

func (o *elasticOutput) Write(records []*model.WalData) error {
	requests := make([]elastic.BulkableRequest, 0, len(records))
	for i := 0; i < len(records); i++ {
		if records[i] != nil {
			if request := o.buildRequests(records[i]); request != nil {
				requests = append(requests, request)
			}
		}
	}
	if len(requests) > 0 {
		bulk := o.client.Bulk().Add(requests...).Refresh("true")
		if _, err := bulk.Do(context.Background()); err != nil {
			return err
		}
	}
	return nil
}

func (o *elasticOutput) buildRequests(data *model.WalData) elastic.BulkableRequest {
	zap.L().Debug("write to elastic", zap.String("table", data.Table), zap.Any("data", data.Data))
	switch data.OperationType {
	case model.INSERT, model.UPDATE:
		return elastic.NewBulkIndexRequest().
			Index(o.cfg.Index).
			Id(data.Data["pk"].(string)).
			Doc(data.Data)
	case model.DELETE:
		return elastic.NewBulkDeleteRequest().
			Index(o.cfg.Index).
			Id(data.Data["pk"].(string))
	}
	return nil
}

func (o *elasticOutput) Close() {
	o.client.Stop()
}
