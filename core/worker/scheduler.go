package worker

import (
	"context"
	"logical/config"
	"logical/core/model"
	"logical/core/worker/output"
	"time"
)

// Scheduler worker manager
type Scheduler struct {
	outputs  []output.Output
	cfg      *config.OutputConfig
	callback func(position uint64)
	Queue    chan []*model.WalData
	records  []*model.WalData
	pos      uint64
	cancel   context.CancelFunc
	done     chan struct{}
}

// StartScheduler Start worker scheduler
func StartScheduler(outputs []string, outputCfg *config.OutputConfig, callback func(position uint64)) *Scheduler {
	var scheduler = &Scheduler{
		Queue:    make(chan []*model.WalData, 20480),
		callback: callback,
		done:     make(chan struct{}),
	}
	for i := 0; i < len(outputs); i++ {
		var target output.Output
		switch outputs[i] {
		case "rabbitmq":
			target = output.NewRabbitOutput(outputCfg.RabbitMQ)
		case "elasticsearch":
			target = output.NewElasticOutput(outputCfg.ElasticSearch)
		case "kafka":
			target = output.NewKafkaOutput(outputCfg.Kafka)
		case "restapi":
			target = &output.RestapiOutput{Cfg: &outputCfg.RestApi}
		case "mongo":
			target = &output.MongoOutput{Cfg: &outputCfg.MongoDB}
		}
		if target != nil {
			scheduler.outputs = append(scheduler.outputs, target)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	scheduler.cancel = cancel
	go scheduler.start(ctx)
	return scheduler
}

// start timer
func (s *Scheduler) start(ctx context.Context) {
	defer close(s.done)
	threshold := 100 * time.Millisecond
	timer := time.NewTimer(threshold)
	for {
		var fresh bool
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			fresh = true
		case records := <-s.Queue:
			for i := 0; i < len(records); i++ {
				if records[i].Pos > s.pos {
					s.pos = records[i].Pos
				}
				s.records = append(s.records, records[i])
			}
			fresh = len(s.records) >= 10000
		}
		if fresh {
			if s.records != nil { // worker data
				for i := 0; i < len(s.outputs); i++ {
					if err := s.outputs[i].Write(s.records); err != nil {
						s.Commit(s.records)
						continue
					}
				}
				s.recovery(s.records)
				s.callback(s.pos)
				s.records = nil
			}
			s.reset(timer, threshold)
		}
	}
}

func (s *Scheduler) recovery(records []*model.WalData) {
	for i := 0; i < len(records); i++ {
		model.PutData(s.records[i])
	}
}

// reset timer
func (s *Scheduler) reset(t *time.Timer, d time.Duration) {
	select {
	case <-t.C:
	default:
	}
	t.Reset(d)
}

// Commit records
func (s *Scheduler) Commit(records []*model.WalData) {
	s.Queue <- records
}

func (s *Scheduler) Stop() {
	s.cancel()
	<-s.done
	for i := 0; i < len(s.outputs); i++ {
		s.outputs[i].Close()
	}
}
