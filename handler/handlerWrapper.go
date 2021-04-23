package handler

import (
	"context"
	"time"

	"logical/conf"
	"logical/handler/output"
	"logical/model"
	"logical/util"
)

type handlerWrapper struct {
	output    output.Output
	dataCh    chan []*model.WalData
	records   []*model.WalData
	maxPos    uint64
	callback  PosCallback
	sub       *conf.SubscribeConfig
	rules     []string
	ruleCache map[string]string
	skipCache map[string]struct{}
	cancel    context.CancelFunc
	done      chan struct{}
}

func (h *handlerWrapper) runloop(ctx context.Context) {
	defer close(h.done)

	timer := time.NewTimer(time.Second)
	for {
		var flush bool

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			flush = true
		case records := <-h.dataCh:
			for _, data := range records {
				if data.Pos > h.maxPos {
					h.maxPos = data.Pos
				}

				if rule, matched := h.filterData(data); matched {
					data.Rule = rule
					h.records = append(h.records, data)
				}

			}
			flush = len(h.records) >= 20000
		}

		if flush {
			_ = h.flush()
			resetTimer(timer, time.Second)
		}
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	// reset timer
	select {
	case <-t.C:
	default:
	}
	t.Reset(d)
}

func (h *handlerWrapper) flush() (err error) {
	defer func() {
		h.callback(h.maxPos)
		h.records = nil
	}()

	if len(h.records) == 0 {
		return nil
	}

	return h.output.Write(h.records...)
}

func (h *handlerWrapper) filterData(data *model.WalData) (matchedRule string, matched bool) {
	if len(data.Data) == 0 {
		return
	}

	if _, skip := h.skipCache[data.Table]; skip {
		return
	}

	matchedRule, matched = h.ruleCache[data.Table]
	if !matched {
		for _, rule := range h.rules {
			if util.MatchSimple(rule, data.Table) {
				matched = true
				matchedRule = rule
				break
			}
		}

		if !matched {
			h.skipCache[data.Table] = struct{}{}
			return
		}
		h.ruleCache[data.Table] = matchedRule
		return
	}
	return
}

func (h *handlerWrapper) Handle(records ...*model.WalData) error {
	h.dataCh <- records
	return nil
}

func (h *handlerWrapper) Stop() {
	h.cancel()
	<-h.done
	h.output.Close()
}
