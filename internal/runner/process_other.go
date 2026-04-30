//go:build !unix

package runner

import (
	"os"
	"os/exec"
)

func prepareCommand(cmd *exec.Cmd) {}

func killProcessTree(process *os.Process) error {
	if process == nil {
		return nil
	}
	if err := process.Kill(); err != nil && !errorsIsProcessDone(err) {
		return err
	}
	return nil
}
