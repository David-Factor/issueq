//go:build windows

package eventcore

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"time"
)

func runCommand(ctx context.Context, command []string, env []string, stdout, stderr *os.File, shutdownGrace time.Duration) error {
	if len(command) == 0 {
		return errors.New("command is empty")
	}
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Env = env
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	return cmd.Run()
}
