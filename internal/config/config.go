// Package config loads and validates issueq YAML configuration.
package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultConfigPath     = "./issueq.yaml"
	DefaultPolling        = 3 * time.Minute
	DefaultQueueBackend   = "sqlite"
	DefaultLeaseDuration  = 30 * time.Minute
	DefaultWorkdir        = "./.issueq"
	DefaultGitHubHost     = "github.com"
	DefaultGitHubTokenEnv = "GITHUB_TOKEN"
)

var defaultEnvPass = []string{"PATH", "HOME"}

// Config is the root issueq YAML configuration.
type Config struct {
	Runner  RunnerConfig  `yaml:"runner"`
	Queue   QueueConfig   `yaml:"queue"`
	Workdir WorkdirConfig `yaml:"workdir"`
	Polling PollingConfig `yaml:"polling"`
	GitHub  GitHubConfig  `yaml:"github"`
	Routes  []RouteConfig `yaml:"routes"`
}

type RunnerConfig struct {
	Name         string    `yaml:"name"`
	Capabilities []string  `yaml:"capabilities"`
	Env          EnvConfig `yaml:"env"`
}

type EnvConfig struct {
	Inherit bool     `yaml:"inherit"`
	Pass    []string `yaml:"pass"`
}

type EnvPassConfig struct {
	Pass []string `yaml:"pass"`
}

type QueueConfig struct {
	Backend              string       `yaml:"backend"`
	SQLite               SQLiteConfig `yaml:"sqlite"`
	MaxGlobalConcurrency int          `yaml:"max_global_concurrency"`
	LeaseDuration        Duration     `yaml:"lease_duration"`
}

type SQLiteConfig struct {
	Path string `yaml:"path"`
}

type WorkdirConfig struct {
	Path string `yaml:"path"`
}

type PollingConfig struct {
	Interval Duration `yaml:"interval"`
}

type GitHubConfig struct {
	Host     string `yaml:"host"`
	Owner    string `yaml:"owner"`
	Repo     string `yaml:"repo"`
	TokenEnv string `yaml:"token_env"`
}

type RouteConfig struct {
	Name      string         `yaml:"name"`
	EventKind string         `yaml:"event_kind"`
	Requires  RequiresConfig `yaml:"requires"`
	Job       JobConfig      `yaml:"job"`
}

type RequiresConfig struct {
	Handoff EventHandoffGateConfig `yaml:"handoff"`
}

type EventHandoffGateConfig struct {
	From         string   `yaml:"from"`
	Decisions    []string `yaml:"decisions"`
	SameTarget   bool     `yaml:"same_target"`
	ExpectedNext bool     `yaml:"expected_next"`
}

type JobConfig struct {
	Kind        string           `yaml:"kind"`
	Command     Command          `yaml:"command"`
	Timeout     Duration         `yaml:"timeout"`
	Concurrency int              `yaml:"concurrency"`
	MaxAttempts int              `yaml:"max_attempts"`
	Priority    int              `yaml:"priority"`
	Env         EnvPassConfig    `yaml:"env"`
	FollowUps   []FollowUpConfig `yaml:"follow_ups"`
}

type FollowUpConfig struct {
	Decision string `yaml:"decision"`
	Kind     string `yaml:"kind"`
	Route    string `yaml:"route"`
}

// Duration wraps time.Duration with YAML string parsing, e.g. "3m".
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode || value.Tag != "!!str" {
		return fmt.Errorf("duration must be a string like \"3m\"")
	}
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value.Value, err)
	}
	d.Duration = parsed
	return nil
}

func (d Duration) String() string {
	return d.Duration.String()
}

// Command is an argv array. YAML shell strings are intentionally rejected.
type Command []string

func (c *Command) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.SequenceNode {
		return fmt.Errorf("command must be a YAML list of argv strings")
	}
	cmd := make([]string, 0, len(value.Content))
	for i, item := range value.Content {
		if item.Kind != yaml.ScalarNode || item.Tag != "!!str" {
			return fmt.Errorf("command[%d] must be a string", i)
		}
		cmd = append(cmd, item.Value)
	}
	*c = cmd
	return nil
}

// ValidateOptions controls context-dependent validation.
type ValidateOptions struct {
	// RequireGitHubToken checks that the environment variable named by github.token_env exists.
	// Use this for commands that contact GitHub.
	RequireGitHubToken bool
}

// LoadFile reads, decodes, defaults, and validates a config file.
func LoadFile(path string) (*Config, error) {
	return LoadFileWithOptions(path, ValidateOptions{})
}

func LoadFileWithOptions(path string, opts ValidateOptions) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	cfg, err := LoadBytesWithOptions(data, opts)
	if err != nil {
		return nil, fmt.Errorf("load config %s: %w", path, err)
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve config path %s: %w", path, err)
	}
	cfg.ResolveRelativePaths(filepath.Dir(absPath))
	return cfg, nil
}

func (c *Config) ResolveRelativePaths(baseDir string) {
	if baseDir == "" {
		return
	}
	c.Queue.SQLite.Path = resolveConfigPath(baseDir, c.Queue.SQLite.Path)
	c.Workdir.Path = resolveConfigPath(baseDir, c.Workdir.Path)
	for i := range c.Routes {
		command := c.Routes[i].Job.Command
		if len(command) > 0 && isExplicitRelativePath(command[0]) {
			c.Routes[i].Job.Command[0] = resolveConfigPath(baseDir, command[0])
		}
	}
}

func resolveConfigPath(baseDir, path string) string {
	if path == "" || path == ":memory:" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Clean(filepath.Join(baseDir, path))
}

func isExplicitRelativePath(path string) bool {
	return strings.HasPrefix(path, "./") || strings.HasPrefix(path, "../")
}

// LoadBytes decodes, defaults, and validates YAML config bytes.
func LoadBytes(data []byte) (*Config, error) {
	return LoadBytesWithOptions(data, ValidateOptions{})
}

func LoadBytesWithOptions(data []byte, opts ValidateOptions) (*Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse YAML: %w", err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(opts); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ApplyDefaults fills unspecified v1 defaults.
func (c *Config) ApplyDefaults() {
	if c.Queue.Backend == "" {
		c.Queue.Backend = DefaultQueueBackend
	}
	if c.Queue.LeaseDuration.Duration == 0 {
		c.Queue.LeaseDuration = Duration{Duration: DefaultLeaseDuration}
	}
	if c.Queue.MaxGlobalConcurrency == 0 {
		c.Queue.MaxGlobalConcurrency = 1
	}
	if c.Workdir.Path == "" {
		c.Workdir.Path = DefaultWorkdir
	}
	if c.Polling.Interval.Duration == 0 {
		c.Polling.Interval = Duration{Duration: DefaultPolling}
	}
	if c.GitHub.Host == "" {
		c.GitHub.Host = DefaultGitHubHost
	}
	if c.GitHub.TokenEnv == "" {
		c.GitHub.TokenEnv = DefaultGitHubTokenEnv
	}
	if c.Runner.Env.Pass == nil {
		c.Runner.Env.Pass = append([]string(nil), defaultEnvPass...)
	}
}

// Validate checks config rules from the v1 spec.
func (c Config) Validate(opts ValidateOptions) error {
	var errs []string

	if strings.TrimSpace(c.GitHub.Owner) == "" {
		errs = append(errs, "github.owner is required")
	}
	if strings.TrimSpace(c.GitHub.Repo) == "" {
		errs = append(errs, "github.repo is required")
	}
	if strings.TrimSpace(c.GitHub.TokenEnv) == "" {
		errs = append(errs, "github.token_env is required")
	} else if !validEnvName(c.GitHub.TokenEnv) {
		errs = append(errs, fmt.Sprintf("github.token_env %q is not a valid environment variable name", c.GitHub.TokenEnv))
	} else if opts.RequireGitHubToken {
		if strings.TrimSpace(os.Getenv(c.GitHub.TokenEnv)) == "" {
			errs = append(errs, fmt.Sprintf("environment variable %s named by github.token_env is not set", c.GitHub.TokenEnv))
		}
	}

	if strings.TrimSpace(c.Queue.Backend) == "" {
		errs = append(errs, "queue.backend is required")
	}
	if c.Queue.Backend != DefaultQueueBackend {
		errs = append(errs, fmt.Sprintf("queue.backend %q is not supported in v1", c.Queue.Backend))
	}
	if strings.TrimSpace(c.Queue.SQLite.Path) == "" {
		errs = append(errs, "queue.sqlite.path is required")
	}
	if c.Queue.MaxGlobalConcurrency <= 0 {
		errs = append(errs, "queue.max_global_concurrency must be positive")
	}
	if c.Queue.LeaseDuration.Duration <= 0 {
		errs = append(errs, "queue.lease_duration must be positive")
	}
	if c.Polling.Interval.Duration <= 0 {
		errs = append(errs, "polling.interval must be positive")
	}
	if strings.TrimSpace(c.Workdir.Path) == "" {
		errs = append(errs, "workdir.path is required")
	}
	errs = append(errs, validateEnvPass("runner.env.pass", c.Runner.Env.Pass, c.GitHub.TokenEnv)...)

	seenRoutes := map[string]struct{}{}
	for _, route := range c.Routes {
		name := strings.TrimSpace(route.Name)
		if name == "" {
			continue
		}
		if _, exists := seenRoutes[name]; exists {
			continue
		}
		seenRoutes[name] = struct{}{}
	}

	validatedRoutes := map[string]struct{}{}
	for i, route := range c.Routes {
		prefix := fmt.Sprintf("routes[%d]", i)
		name := strings.TrimSpace(route.Name)
		if name == "" {
			errs = append(errs, prefix+".name is required")
		} else if _, exists := validatedRoutes[name]; exists {
			errs = append(errs, fmt.Sprintf("%s.name %q is duplicated", prefix, name))
		} else {
			validatedRoutes[name] = struct{}{}
		}

		if strings.TrimSpace(route.EventKind) == "" {
			errs = append(errs, prefix+".event_kind is required; bridge/label routes are not supported")
		}
		errs = append(errs, validateEventRequires(prefix+".requires", route.Requires, seenRoutes)...)

		jobPrefix := prefix + ".job"
		if strings.TrimSpace(route.Job.Kind) == "" {
			errs = append(errs, jobPrefix+".kind is required")
		}
		if len(route.Job.Command) == 0 {
			errs = append(errs, jobPrefix+".command is required and must be a non-empty argv list")
		} else {
			for j, part := range route.Job.Command {
				if strings.TrimSpace(part) == "" {
					errs = append(errs, fmt.Sprintf("%s.command[%d] must not be empty", jobPrefix, j))
				}
			}
		}
		if route.Job.Timeout.Duration <= 0 {
			errs = append(errs, jobPrefix+".timeout must be positive")
		}
		if route.Job.Concurrency <= 0 {
			errs = append(errs, jobPrefix+".concurrency must be positive")
		}
		if route.Job.MaxAttempts <= 0 {
			errs = append(errs, jobPrefix+".max_attempts must be positive")
		}
		errs = append(errs, validateEnvPass(jobPrefix+".env.pass", route.Job.Env.Pass, c.GitHub.TokenEnv)...)
		for _, follow := range route.Job.FollowUps {
			if strings.TrimSpace(follow.Decision) == "" {
				errs = append(errs, jobPrefix+".follow_ups.decision is required")
			}
			if strings.TrimSpace(follow.Kind) == "" {
				errs = append(errs, jobPrefix+".follow_ups.kind is required")
			}
			if strings.TrimSpace(follow.Route) == "" {
				errs = append(errs, jobPrefix+".follow_ups.route is required")
			} else if _, ok := seenRoutes[follow.Route]; !ok {
				errs = append(errs, fmt.Sprintf("%s.follow_ups.route references unknown route %q", jobPrefix, follow.Route))
			}
		}
	}

	if len(errs) > 0 {
		return ValidationError(errs)
	}
	return nil
}

// ValidationError is a deterministic, user-readable set of config errors.
type ValidationError []string

func (e ValidationError) Error() string {
	return "config validation failed: " + strings.Join([]string(e), "; ")
}

var envNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validEnvName(name string) bool {
	return envNameRE.MatchString(name)
}

func validateEnvPass(path string, names []string, tokenEnv string) []string {
	var errs []string
	seen := map[string]struct{}{}
	for i, name := range names {
		if !validEnvName(name) {
			errs = append(errs, fmt.Sprintf("%s[%d] %q is not a valid environment variable name", path, i, name))
		}
		if name == tokenEnv {
			errs = append(errs, fmt.Sprintf("%s must not include github.token_env %q", path, tokenEnv))
		}
		if _, exists := seen[name]; exists {
			errs = append(errs, fmt.Sprintf("%s contains duplicate %q", path, name))
		}
		seen[name] = struct{}{}
	}
	return errs
}

func validateEventRequires(path string, requires RequiresConfig, routeNames map[string]struct{}) []string {
	var errs []string
	handoff := requires.Handoff
	if strings.TrimSpace(handoff.From) != "" {
		if _, ok := routeNames[handoff.From]; !ok {
			errs = append(errs, fmt.Sprintf("%s.handoff.from references unknown route %q", path, handoff.From))
		}
		if len(handoff.Decisions) == 0 {
			errs = append(errs, path+".handoff.decisions is required when handoff.from is set")
		}
	}
	errs = append(errs, validateStringList(path+".handoff.decisions", handoff.Decisions)...)
	return errs
}

func validateStringList(path string, values []string) []string {
	var errs []string
	seen := map[string]struct{}{}
	for i, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			errs = append(errs, fmt.Sprintf("%s[%d] must not be empty", path, i))
		}
		if _, exists := seen[trimmed]; exists {
			errs = append(errs, fmt.Sprintf("%s contains duplicate %q", path, value))
		}
		seen[trimmed] = struct{}{}
	}
	return errs
}
