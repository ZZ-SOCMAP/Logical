package util

// WithRetry retry util job succeeded or retry count limit exceed
func WithRetry(retry int, job func() error) error {
	var retrys int
	for {
		retrys++
		if err := job(); err != nil {
			if retry == -1 || (retry > 0 && retrys <= retry) {
				continue
			}
			return err
		}
		return nil
	}
}
