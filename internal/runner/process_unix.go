//go:build unix

package runner

import (
	"os"
	"os/exec"
	"syscall"
)

func prepareCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killProcessTree(process *os.Process) error {
	if process == nil {
		return nil
	}
	pgid, err := syscall.Getpgid(process.Pid)
	if err == nil && pgid > 1 {
		killErr := syscall.Kill(-pgid, syscall.SIGKILL)
		if killErr == nil || killErr == syscall.ESRCH {
			return nil
		}
		if childErr := process.Kill(); childErr != nil && !errorsIsProcessDone(childErr) {
			return killErr
		}
		return killErr
	}
	if err := process.Kill(); err != nil && !errorsIsProcessDone(err) {
		return err
	}
	return nil
}
