package handler

import (
	"context"
	model2 "logical/core/model"
	"time"

	"logical/config"
)

type wrapper struct {
	output    Output
	dataCh    chan []*model2.WalData
	records   []*model2.WalData
	maxPos    uint64
	callback  PosCallback
	capture   *config.Capture
	ruleCache map[string]string
	skipCache map[string]struct{}
	cancel    context.CancelFunc
	done      chan struct{}
}

func (h *wrapper) runloop(ctx context.Context) {
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

func (h *wrapper) flush() (err error) {
	defer func() {
		h.callback(h.maxPos)
		h.records = nil
	}()

	if len(h.records) == 0 {
		return nil
	}

	return h.output.Write(h.records...)
}

func (h *wrapper) filterData(data *model2.WalData) (matchedRule string, matched bool) {
	if len(data.Data) == 0 {
		return
	}
	if _, skip := h.skipCache[data.Table]; skip {
		return
	}
	matchedRule, matched = h.ruleCache[data.Table]
	if !matched {
		for _, rule := range h.capture.Tables {
			if match(rule, data.Table) {
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

func (h *wrapper) Handle(records ...*model2.WalData) error {
	h.dataCh <- records
	return nil
}

func (h *wrapper) Stop() {
	h.cancel()
	<-h.done
	h.output.Close()
}
