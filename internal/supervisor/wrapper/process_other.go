//go:build !linux

package wrapper

import (
	"errors"
	"os/exec"
	"time"
)

func prepareCommand(cmd *exec.Cmd)                     {}
func processGroupID(pid int) int                       { return 0 }
func processStartedAt(pid int) time.Time               { return time.Time{} }
func processMatches(pid int, startedAt time.Time) bool { return false }
func processExists(pid int) bool                       { return false }
func terminateProcess(pid int) error {
	return errors.New("wrapper process verification is only implemented on linux")
}
func killProcess(pid int) error { return nil }
