package dump

import (
	"fmt"
	"io"
	handler2 "logical/core/handler"
	"os"
	"os/exec"

	"logical/config"
)

// Dumper dump database
type Dumper struct {
	dump    string
	capture *config.Capture
}

// New create a Dumper
func New(pgDump string, sub *config.Capture) *Dumper {
	if pgDump == "" {
		pgDump = "pg_dump"
	}
	path, _ := exec.LookPath(pgDump)
	return &Dumper{dump: path, capture: sub}
}

// Dump database with snapshot, parse sql then write to handler
func (d *Dumper) Dump(snapshotID string, h handler2.Handler) error {
	if d.dump == "" {
		return nil
	}
	args := make([]string, 0, 16)
	args = append(args, fmt.Sprintf("--host=%s", d.capture.DbHost))
	args = append(args, fmt.Sprintf("--port=%d", d.capture.DbPort))
	args = append(args, fmt.Sprintf("--username=%s", d.capture.DbUser))
	args = append(args, d.capture.DbName)
	args = append(args, "--data-only")
	args = append(args, "--column-inserts")
	for i := 0; i < len(d.capture.Tables); i++ {
		args = append(args, fmt.Sprintf(`--table=%s`, d.capture.Tables[i]))
	}
	args = append(args, fmt.Sprintf("--snapshot=%s", snapshotID))
	cmd := exec.Command(d.dump, args...)
	if d.capture.DbPass != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", d.capture.DbPass))
	}
	var r, w = io.Pipe()
	cmd.Stdout = w
	cmd.Stderr = os.Stderr
	var errCh = make(chan error)
	var parser = newParser(r)
	go func() {
		err := parser.parse(h)
		errCh <- err
	}()
	err := cmd.Run()
	_ = w.CloseWithError(err)
	err = <-errCh
	return err
}
