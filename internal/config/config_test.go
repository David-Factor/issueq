package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidEventConfigLoads(t *testing.T) {
	cfg, err := LoadBytes([]byte(minimalConfig()))
	if err != nil {
		t.Fatalf("LoadBytes() error = %v", err)
	}
	if cfg.GitHub.Owner != "example-org" || cfg.GitHub.Repo != "example-repo" {
		t.Fatalf("unexpected github config: %#v", cfg.GitHub)
	}
	if len(cfg.Routes) != 1 || cfg.Routes[0].EventKind != "triage" {
		t.Fatalf("routes = %#v", cfg.Routes)
	}
}

func TestLoadFileResolvesRelativePathsFromConfigDir(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "issueq.yaml")
	configText := `github:
  owner: example-org
  repo: example-repo
queue:
  sqlite:
    path: ./queue/issueq.db
workdir:
  path: ./.issueq
routes:
  - name: explicit-current
    event_kind: explicit-current
    job:
      kind: code
      command: ["./tasks/code.sh", "./unchanged-arg"]
      timeout: 10m
      concurrency: 1
      max_attempts: 2
  - name: explicit-parent
    event_kind: explicit-parent
    job:
      kind: code
      command: ["../bin/code.sh"]
      timeout: 10m
      concurrency: 1
      max_attempts: 2
  - name: bare-command
    event_kind: bare-command
    job:
      kind: code
      command: ["bash", "-lc", "./tasks/code.sh"]
      timeout: 10m
      concurrency: 1
      max_attempts: 2
`
	if err := os.WriteFile(configPath, []byte(configText), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadFile(configPath)
	if err != nil {
		t.Fatalf("LoadFile() error = %v", err)
	}

	if cfg.Queue.SQLite.Path != filepath.Join(dir, "queue", "issueq.db") {
		t.Fatalf("sqlite path = %q", cfg.Queue.SQLite.Path)
	}
	if cfg.Workdir.Path != filepath.Join(dir, ".issueq") {
		t.Fatalf("workdir path = %q", cfg.Workdir.Path)
	}
	if cfg.Routes[0].Job.Command[0] != filepath.Join(dir, "tasks", "code.sh") {
		t.Fatalf("current command = %#v", cfg.Routes[0].Job.Command)
	}
	if cfg.Routes[0].Job.Command[1] != "./unchanged-arg" {
		t.Fatalf("command arg resolved unexpectedly: %#v", cfg.Routes[0].Job.Command)
	}
	if cfg.Routes[1].Job.Command[0] != filepath.Clean(filepath.Join(dir, "..", "bin", "code.sh")) {
		t.Fatalf("parent command = %#v", cfg.Routes[1].Job.Command)
	}
	if got := cfg.Routes[2].Job.Command; got[0] != "bash" || got[2] != "./tasks/code.sh" {
		t.Fatalf("bare command changed = %#v", got)
	}
}

func TestLoadFileLeavesMemorySQLitePath(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "issueq.yaml")
	configText := strings.Replace(minimalConfig(), "    path: ./issueq.db", "    path: ':memory:'", 1)
	if err := os.WriteFile(configPath, []byte(configText), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFile(configPath)
	if err != nil {
		t.Fatalf("LoadFile() error = %v", err)
	}
	if cfg.Queue.SQLite.Path != ":memory:" {
		t.Fatalf("sqlite path = %q, want :memory:", cfg.Queue.SQLite.Path)
	}
}

func TestDefaults(t *testing.T) {
	cfg, err := LoadBytes([]byte(minimalConfig()))
	if err != nil {
		t.Fatalf("LoadBytes() error = %v", err)
	}

	if cfg.Queue.Backend != DefaultQueueBackend {
		t.Fatalf("queue backend = %q", cfg.Queue.Backend)
	}
	if cfg.Queue.LeaseDuration.Duration != DefaultLeaseDuration {
		t.Fatalf("lease duration = %s", cfg.Queue.LeaseDuration)
	}
	if cfg.Polling.Interval.Duration != DefaultPolling {
		t.Fatalf("polling interval = %s", cfg.Polling.Interval)
	}
	if cfg.Workdir.Path != DefaultWorkdir {
		t.Fatalf("workdir = %q", cfg.Workdir.Path)
	}
	if cfg.GitHub.Host != DefaultGitHubHost || cfg.GitHub.TokenEnv != DefaultGitHubTokenEnv {
		t.Fatalf("github defaults = %#v", cfg.GitHub)
	}
	if cfg.Runner.Env.Inherit {
		t.Fatal("runner.env.inherit defaulted true; want false")
	}
	if got := strings.Join(cfg.Runner.Env.Pass, ","); got != "PATH,HOME" {
		t.Fatalf("runner.env.pass = %q", got)
	}
}

func TestValidationFailures(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "missing owner",
			yaml:    strings.Replace(minimalConfig(), "  owner: example-org\n", "", 1),
			wantErr: "github.owner is required",
		},
		{
			name:    "missing repo",
			yaml:    strings.Replace(minimalConfig(), "  repo: example-repo\n", "", 1),
			wantErr: "github.repo is required",
		},
		{
			name: "duplicate route names",
			yaml: minimalConfig() + `
  - name: triage
    event_kind: triage-again
    job:
      kind: event
      command: ["./tasks/code.sh"]
      timeout: 10m
      concurrency: 1
      max_attempts: 2
`,
			wantErr: `routes[1].name "triage" is duplicated`,
		},
		{
			name:    "empty route name",
			yaml:    replaceOnce("  - name: triage", "  - name: ''"),
			wantErr: "routes[0].name is required",
		},
		{
			name:    "missing event kind rejects label scheduler route",
			yaml:    strings.Replace(minimalConfig(), "    event_kind: triage\n", "", 1),
			wantErr: "routes[0].event_kind is required; bridge/label routes are not supported",
		},
		{
			name:    "legacy when label predicates are unknown",
			yaml:    strings.Replace(minimalConfig(), "    event_kind: triage\n", "    when:\n      labels_include: [agent-triage]\n", 1),
			wantErr: "field when not found",
		},
		{
			name:    "legacy label actions are unknown",
			yaml:    strings.Replace(minimalConfig(), "      max_attempts: 2\n", "      max_attempts: 2\n      on_success:\n        labels_add: [agent-running]\n", 1),
			wantErr: "field on_success not found",
		},
		{
			name:    "empty kind",
			yaml:    replaceOnce("      kind: event", "      kind: ''"),
			wantErr: "routes[0].job.kind is required",
		},
		{
			name:    "empty command",
			yaml:    replaceOnce("      command: [\"./tasks/triage.sh\"]", "      command: []"),
			wantErr: "routes[0].job.command is required",
		},
		{
			name:    "empty command element",
			yaml:    replaceOnce("      command: [\"./tasks/triage.sh\"]", "      command: ['']"),
			wantErr: "routes[0].job.command[0] must not be empty",
		},
		{
			name:    "non-positive timeout",
			yaml:    replaceOnce("      timeout: 10m", "      timeout: 0s"),
			wantErr: "routes[0].job.timeout must be positive",
		},
		{
			name:    "missing timeout",
			yaml:    strings.Replace(minimalConfig(), "      timeout: 10m\n", "", 1),
			wantErr: "routes[0].job.timeout must be positive",
		},
		{
			name:    "non-positive concurrency",
			yaml:    replaceOnce("      concurrency: 1", "      concurrency: 0"),
			wantErr: "routes[0].job.concurrency must be positive",
		},
		{
			name:    "non-positive max attempts",
			yaml:    replaceOnce("      max_attempts: 2", "      max_attempts: 0"),
			wantErr: "routes[0].job.max_attempts must be positive",
		},
		{
			name:    "invalid runner env name",
			yaml:    strings.Replace(minimalConfig(), "github:\n", "runner:\n  env:\n    pass: [BAD-NAME]\n\ngithub:\n", 1),
			wantErr: `runner.env.pass[0] "BAD-NAME" is not a valid environment variable name`,
		},
		{
			name:    "invalid route env name",
			yaml:    strings.Replace(minimalConfig(), "      max_attempts: 2\n", "      max_attempts: 2\n      env:\n        pass: [1BAD]\n", 1),
			wantErr: `routes[0].job.env.pass[0] "1BAD" is not a valid environment variable name`,
		},
		{
			name:    "runner env passes github token",
			yaml:    strings.Replace(minimalConfig(), "github:\n", "runner:\n  env:\n    pass: [GITHUB_TOKEN]\n\ngithub:\n", 1),
			wantErr: `runner.env.pass must not include github.token_env "GITHUB_TOKEN"`,
		},
		{
			name:    "route env passes github token",
			yaml:    strings.Replace(minimalConfig(), "      max_attempts: 2\n", "      max_attempts: 2\n      env:\n        pass: [GITHUB_TOKEN]\n", 1),
			wantErr: `routes[0].job.env.pass must not include github.token_env "GITHUB_TOKEN"`,
		},
		{
			name:    "empty sqlite path",
			yaml:    replaceOnce("    path: ./issueq.db", "    path: ''"),
			wantErr: "queue.sqlite.path is required",
		},
		{
			name:    "unsupported queue backend",
			yaml:    strings.Replace(minimalConfig(), "queue:\n", "queue:\n  backend: postgres\n", 1),
			wantErr: `queue.backend "postgres" is not supported in v1`,
		},
		{
			name:    "unknown required handoff from route",
			yaml:    strings.Replace(minimalConfig(), "    job:\n", "    requires:\n      handoff:\n        from: missing-route\n        decisions: [fix_candidate]\n    job:\n", 1),
			wantErr: `routes[0].requires.handoff.from references unknown route "missing-route"`,
		},
		{
			name:    "required handoff missing decisions",
			yaml:    strings.Replace(minimalConfig(), "    job:\n", "    requires:\n      handoff:\n        from: triage\n    job:\n", 1),
			wantErr: "routes[0].requires.handoff.decisions is required when handoff.from is set",
		},
		{
			name:    "empty required handoff decision",
			yaml:    strings.Replace(minimalConfig(), "    job:\n", "    requires:\n      handoff:\n        decisions: ['']\n    job:\n", 1),
			wantErr: "routes[0].requires.handoff.decisions[0] must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadBytes([]byte(tt.yaml))
			if err == nil {
				t.Fatal("LoadBytes() error = nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestCommandStringRejected(t *testing.T) {
	_, err := LoadBytes([]byte(replaceOnce("      command: [\"./tasks/triage.sh\"]", "      command: ./tasks/triage.sh")))
	if err == nil {
		t.Fatal("LoadBytes() error = nil")
	}
	if !strings.Contains(err.Error(), "command must be a YAML list of argv strings") {
		t.Fatalf("error = %v", err)
	}
}

func TestRequireGitHubToken(t *testing.T) {
	t.Setenv("ISSUEQ_TEST_TOKEN", "")
	_ = os.Unsetenv("ISSUEQ_TEST_TOKEN")

	cfgText := strings.Replace(minimalConfig(), "  token_env: GITHUB_TOKEN", "  token_env: ISSUEQ_TEST_TOKEN", 1)
	_, err := LoadBytesWithOptions([]byte(cfgText), ValidateOptions{RequireGitHubToken: true})
	if err == nil {
		t.Fatal("LoadBytesWithOptions() error = nil")
	}
	if !strings.Contains(err.Error(), "environment variable ISSUEQ_TEST_TOKEN named by github.token_env is not set") {
		t.Fatalf("error = %v", err)
	}

	t.Setenv("ISSUEQ_TEST_TOKEN", "   ")
	_, err = LoadBytesWithOptions([]byte(cfgText), ValidateOptions{RequireGitHubToken: true})
	if err == nil {
		t.Fatal("LoadBytesWithOptions() error = nil for whitespace token")
	}

	t.Setenv("ISSUEQ_TEST_TOKEN", "secret")
	if _, err := LoadBytesWithOptions([]byte(cfgText), ValidateOptions{RequireGitHubToken: true}); err != nil {
		t.Fatalf("LoadBytesWithOptions() error = %v", err)
	}
}

func TestDurationParsing(t *testing.T) {
	cfg, err := LoadBytes([]byte(replaceOnce("      timeout: 10m", "      timeout: 90m")))
	if err != nil {
		t.Fatalf("LoadBytes() error = %v", err)
	}
	if cfg.Routes[0].Job.Timeout.Duration != 90*time.Minute {
		t.Fatalf("timeout = %s", cfg.Routes[0].Job.Timeout)
	}

	_, err = LoadBytes([]byte(replaceOnce("      timeout: 10m", "      timeout: nope")))
	if err == nil || !strings.Contains(err.Error(), `invalid duration "nope"`) {
		t.Fatalf("error = %v, want invalid duration", err)
	}
}

func replaceOnce(old, new string) string {
	return strings.Replace(minimalConfig(), old, new, 1)
}

func minimalConfig() string {
	return `github:
  owner: example-org
  repo: example-repo
  token_env: GITHUB_TOKEN
queue:
  sqlite:
    path: ./issueq.db
routes:
  - name: triage
    event_kind: triage
    job:
      kind: event
      command: ["./tasks/triage.sh"]
      timeout: 10m
      concurrency: 1
      max_attempts: 2
`
}
