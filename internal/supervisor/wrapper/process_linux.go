//go:build linux

package wrapper

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func prepareCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func processGroupID(pid int) int {
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		return 0
	}
	return pgid
}

func processStartedAt(pid int) time.Time {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return time.Time{}
	}
	fields := strings.Fields(string(data))
	if len(fields) < 22 {
		return time.Time{}
	}
	// Linux /proc starttime is clock ticks since boot. Store it in the
	// nanosecond field of a zero-date time as a conservative fingerprint; it is
	// not a wall clock timestamp but is stable enough for equality checks.
	ticks, err := strconv.ParseInt(fields[21], 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(0, ticks).UTC()
}

func processMatches(pid int, startedAt time.Time) bool {
	if startedAt.IsZero() {
		return false
	}
	current := processStartedAt(pid)
	return !current.IsZero() && current.Equal(startedAt)
}

func processExists(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}

func killProcessGroup(pgid int) error {
	if pgid <= 1 {
		return nil
	}
	err := syscall.Kill(-pgid, syscall.SIGKILL)
	if err == syscall.ESRCH {
		return nil
	}
	return err
}

func terminateProcess(pid int) error {
	if pid <= 0 {
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
		return err
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !processExists(pid) {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return killProcess(pid)
}

func killProcess(pid int) error {
	if pid <= 0 {
		return nil
	}
	err := syscall.Kill(pid, syscall.SIGKILL)
	if err == syscall.ESRCH {
		return nil
	}
	return err
}
