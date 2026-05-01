//go:build !unix

package jobwrapper

import (
	"os"
	"os/exec"
)

func prepareCommand(cmd *exec.Cmd) {}

func processGroupID(pid int) int { return 0 }

func killProcessGroup(process *os.Process) error {
	if process == nil {
		return nil
	}
	if err := process.Kill(); err != nil && !errorsIsProcessDone(err) {
		return err
	}
	return nil
}

func errorsIsProcessDone(err error) bool {
	if err == nil {
		return true
	}
	return err == os.ErrProcessDone
}
