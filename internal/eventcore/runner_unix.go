//go:build !windows

package eventcore

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"time"
)

func runCommand(ctx context.Context, command []string, env []string, stdout, stderr *os.File, shutdownGrace time.Duration) error {
	if len(command) == 0 {
		return errors.New("command is empty")
	}
	if shutdownGrace <= 0 {
		shutdownGrace = 5 * time.Second
	}
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Env = env
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}

	pid := cmd.Process.Pid
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		pgid = pid
	}
	_ = syscall.Kill(-pgid, syscall.SIGTERM)

	var waitErr error
	waited := false
	deadline := time.NewTimer(shutdownGrace)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if waited && processGroupGone(pgid) {
			if waitErr != nil {
				return waitErr
			}
			return ctx.Err()
		}
		select {
		case err := <-done:
			waitErr = err
			waited = true
		case <-ticker.C:
		case <-deadline.C:
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
			if !waited {
				waitErr = <-done
			}
			return waitErr
		}
	}
}

func processGroupGone(pgid int) bool {
	err := syscall.Kill(-pgid, 0)
	return errors.Is(err, syscall.ESRCH)
}
