package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"issueq/internal/model"
)

func TestRunCancellationIsDistinctFromTimeout(t *testing.T) {
	script := writeScript(t, "#!/bin/sh\nsleep 5\n")
	cfg := testConfig(t, []string{script})
	ctx, cancel := context.WithCancelCause(t.Context())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel(errors.New("shutdown"))
	}()
	res := Run(ctx, cfg, cfg.Routes[0], testJob(), testIssue("title"), model.RunnerInfo{})
	if !res.Cancelled || res.TimedOut || res.Error == nil || !strings.Contains(res.Error.Error(), "shutdown") {
		t.Fatalf("cancel result = %#v", res)
	}
}

func TestRunTimeoutKillsProcessTreeUnix(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix process-group behavior only")
	}
	marker := filepath.Join(t.TempDir(), "grandchild.pid")
	script := writeScript(t, `#!/bin/sh
marker="$1"
(sh -c 'echo $$ > "$1"; trap "" TERM; sleep 30' _ "$marker") &
sleep 30
`)
	cfg := testConfig(t, []string{script, marker})
	cfg.Routes[0].Job.Timeout.Duration = 200 * time.Millisecond
	res := Run(t.Context(), cfg, cfg.Routes[0], testJob(), testIssue("title"), model.RunnerInfo{})
	if !res.TimedOut || res.Cancelled {
		t.Fatalf("timeout result = %#v", res)
	}
	pid := waitForPIDFile(t, marker)
	for i := 0; i < 50; i++ {
		if !processAlive(pid) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("grandchild pid %d still alive after timeout", pid)
}

func waitForPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, convErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if convErr == nil {
				return pid
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("pid file %s not written", path)
	return 0
}

func processAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	if err := process.Signal(os.Signal(nil)); err != nil {
		return false
	}
	return true
}
