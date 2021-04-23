package dump

import (
	"fmt"
	"io"
	"os"
	"os/exec"

	"logical/conf"
	"logical/handler"
)

// Dumper dump database
type Dumper struct {
	pgDump string
	sub    *conf.SubscribeConfig
}

// New create a Dumper
func New(pgDump string, sub *conf.SubscribeConfig) *Dumper {
	if pgDump == "" {
		pgDump = "pg_dump"
	}
	path, _ := exec.LookPath(pgDump)
	return &Dumper{pgDump: path, sub: sub}
}

// Dump database with snapshot, parse sql then write to handler
func (d *Dumper) Dump(snapshotID string, h handler.Handler) error {

	if d.pgDump == "" {
		return nil
	}

	args := make([]string, 0, 16)

	// Common args
	args = append(args, fmt.Sprintf("--host=%s", d.sub.DbHost))
	args = append(args, fmt.Sprintf("--port=%d", d.sub.DbPort))
	args = append(args, fmt.Sprintf("--username=%s", d.sub.DbUser))
	args = append(args, d.sub.DbName)
	args = append(args, "--data-only")
	args = append(args, "--column-inserts")
	//args = append(args, fmt.Sprintf("--schema=%s", d.sub.Schema))
	for _, rule := range d.sub.Tables {
		args = append(args, fmt.Sprintf(`--table=%s`, rule))
	}
	args = append(args, fmt.Sprintf("--snapshot=%s", snapshotID))

	cmd := exec.Command(d.pgDump, args...)
	if d.sub.DbPass != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", d.sub.DbPass))
	}
	r, w := io.Pipe()

	cmd.Stdout = w
	cmd.Stderr = os.Stderr

	errCh := make(chan error)
	parser := newParser(r)
	go func() {
		err := parser.parse(h)
		errCh <- err
	}()

	err := cmd.Run()
	_ = w.CloseWithError(err)

	err = <-errCh
	return err
}
