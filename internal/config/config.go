// Package config loads and validates issueq YAML configuration.
package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
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

	HandoffFreshnessNone                = "none"
	HandoffFreshnessSourceUnchanged     = "source_unchanged"
	HandoffFreshnessTargetHeadUnchanged = "target_head_unchanged"

	AttemptScopeLegacy  = "legacy"
	AttemptScopeHandoff = "handoff"
	AttemptScopeIssue   = "issue"
	AttemptScopePRHead  = "pr_head"
	AttemptScopeCIHead  = "ci_head"
)

var defaultEnvPass = []string{"PATH", "HOME"}

// Config is the root issueq YAML configuration.
type Config struct {
	Runner         RunnerConfig   `yaml:"runner"`
	Queue          QueueConfig    `yaml:"queue"`
	Workdir        WorkdirConfig  `yaml:"workdir"`
	Polling        PollingConfig  `yaml:"polling"`
	GitHub         GitHubConfig   `yaml:"github"`
	TerminalLabels []string       `yaml:"terminal_labels"`
	Workflow       WorkflowConfig `yaml:"workflow"`
	Routes         []RouteConfig  `yaml:"routes"`
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

type WorkflowConfig struct {
	MaxTransitionsPerIssue int          `yaml:"max_transitions_per_issue"`
	OnTransitionsExceeded  ActionConfig `yaml:"on_transitions_exceeded"`
}

type RouteConfig struct {
	Name string          `yaml:"name"`
	When PredicateConfig `yaml:"when"`
	Gate GateConfig      `yaml:"gate"`
	Job  JobConfig       `yaml:"job"`
}

type GateConfig struct {
	Handoff HandoffGateConfig `yaml:"handoff"`
	OnBlock ActionConfig      `yaml:"on_block"`
}

type HandoffGateConfig struct {
	Required  bool                   `yaml:"required"`
	From      []string               `yaml:"from"`
	Decisions []string               `yaml:"decisions"`
	NextRoute HandoffNextRouteConfig `yaml:"next_route"`
	Freshness string                 `yaml:"freshness"`
}

type HandoffNextRouteMode string

const (
	HandoffNextRouteDisabled HandoffNextRouteMode = ""
	HandoffNextRouteCurrent  HandoffNextRouteMode = "current"
	HandoffNextRouteExact    HandoffNextRouteMode = "exact"
)

type HandoffNextRouteConfig struct {
	Mode  HandoffNextRouteMode
	Value string
}

func (c *HandoffNextRouteConfig) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("next_route must be a boolean or string")
	}
	switch value.Tag {
	case "!!bool":
		parsed, err := strconv.ParseBool(value.Value)
		if err != nil {
			return fmt.Errorf("next_route boolean must be true or false")
		}
		if parsed {
			*c = HandoffNextRouteConfig{Mode: HandoffNextRouteCurrent}
		} else {
			*c = HandoffNextRouteConfig{}
		}
		return nil
	case "!!str":
		if strings.TrimSpace(value.Value) == "" {
			return fmt.Errorf("next_route string must not be empty")
		}
		*c = HandoffNextRouteConfig{Mode: HandoffNextRouteExact, Value: value.Value}
		return nil
	default:
		return fmt.Errorf("next_route must be a boolean or string")
	}
}

type PredicateConfig struct {
	LabelsInclude []string `yaml:"labels_include"`
	LabelsExclude []string `yaml:"labels_exclude"`
}

type JobConfig struct {
	Kind               string        `yaml:"kind"`
	Command            Command       `yaml:"command"`
	Timeout            Duration      `yaml:"timeout"`
	Concurrency        int           `yaml:"concurrency"`
	MaxAttempts        int           `yaml:"max_attempts"`
	Priority           int           `yaml:"priority"`
	AttemptScope       string        `yaml:"attempt_scope"`
	Env                EnvPassConfig `yaml:"env"`
	OnStart            ActionConfig  `yaml:"on_start"`
	OnSuccess          ActionConfig  `yaml:"on_success"`
	OnFailure          ActionConfig  `yaml:"on_failure"`
	OnAttemptsExceeded ActionConfig  `yaml:"on_attempts_exceeded"`
}

type ActionConfig struct {
	LabelsAdd    []string `yaml:"labels_add"`
	LabelsRemove []string `yaml:"labels_remove"`
	Comment      string   `yaml:"comment"`
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
	if c.Workflow.MaxTransitionsPerIssue == 0 {
		c.Workflow.MaxTransitionsPerIssue = 10
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
	if c.Workflow.MaxTransitionsPerIssue < 0 {
		errs = append(errs, "workflow.max_transitions_per_issue must be non-negative")
	}

	errs = append(errs, validateEnvPass("runner.env.pass", c.Runner.Env.Pass, c.GitHub.TokenEnv)...)
	errs = append(errs, validateAction("workflow.on_transitions_exceeded", c.Workflow.OnTransitionsExceeded)...)

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

		errs = append(errs, validatePredicate(prefix+".when", route.When)...)
		errs = append(errs, validateGate(prefix+".gate", route.Gate, seenRoutes)...)

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
		if scope := strings.TrimSpace(route.Job.AttemptScope); scope != "" && !validAttemptScope(scope) {
			errs = append(errs, fmt.Sprintf("%s.attempt_scope %q is not supported", jobPrefix, route.Job.AttemptScope))
		}
		errs = append(errs, validateEnvPass(jobPrefix+".env.pass", route.Job.Env.Pass, c.GitHub.TokenEnv)...)
		errs = append(errs, validateAction(jobPrefix+".on_start", route.Job.OnStart)...)
		errs = append(errs, validateAction(jobPrefix+".on_success", route.Job.OnSuccess)...)
		errs = append(errs, validateAction(jobPrefix+".on_failure", route.Job.OnFailure)...)
		errs = append(errs, validateAction(jobPrefix+".on_attempts_exceeded", route.Job.OnAttemptsExceeded)...)
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

func validatePredicate(path string, predicate PredicateConfig) []string {
	var errs []string
	include := map[string]struct{}{}
	for _, label := range predicate.LabelsInclude {
		if _, exists := include[label]; exists {
			errs = append(errs, fmt.Sprintf("%s.labels_include contains duplicate %q", path, label))
		}
		include[label] = struct{}{}
	}
	exclude := map[string]struct{}{}
	for _, label := range predicate.LabelsExclude {
		if _, exists := exclude[label]; exists {
			errs = append(errs, fmt.Sprintf("%s.labels_exclude contains duplicate %q", path, label))
		}
		exclude[label] = struct{}{}
	}

	var conflicts []string
	for label := range include {
		if _, exists := exclude[label]; exists {
			conflicts = append(conflicts, label)
		}
	}
	sort.Strings(conflicts)
	for _, label := range conflicts {
		errs = append(errs, fmt.Sprintf("%s includes and excludes label %q", path, label))
	}
	return errs
}

func validateGate(path string, gate GateConfig, routeNames map[string]struct{}) []string {
	var errs []string
	errs = append(errs, validateAction(path+".on_block", gate.OnBlock)...)

	handoff := gate.Handoff
	if strings.TrimSpace(handoff.Freshness) != "" && !validHandoffFreshness(handoff.Freshness) {
		errs = append(errs, fmt.Sprintf("%s.handoff.freshness %q is not supported", path, handoff.Freshness))
	}
	if handoff.Required && len(handoff.From) == 0 {
		errs = append(errs, path+".handoff.from is required when handoff.required is true")
	}
	errs = append(errs, validateStringList(path+".handoff.from", handoff.From)...)
	errs = append(errs, validateStringList(path+".handoff.decisions", handoff.Decisions)...)
	for _, from := range handoff.From {
		trimmed := strings.TrimSpace(from)
		if trimmed == "" {
			continue
		}
		if _, ok := routeNames[trimmed]; !ok {
			errs = append(errs, fmt.Sprintf("%s.handoff.from references unknown route %q", path, from))
		}
	}
	if handoff.NextRoute.Mode == HandoffNextRouteExact && strings.TrimSpace(handoff.NextRoute.Value) == "" {
		errs = append(errs, path+".handoff.next_route must not be empty")
	}
	return errs
}

func validHandoffFreshness(value string) bool {
	switch value {
	case HandoffFreshnessNone, HandoffFreshnessSourceUnchanged, HandoffFreshnessTargetHeadUnchanged:
		return true
	default:
		return false
	}
}

func validAttemptScope(value string) bool {
	switch value {
	case AttemptScopeLegacy, AttemptScopeHandoff, AttemptScopeIssue, AttemptScopePRHead, AttemptScopeCIHead:
		return true
	default:
		return false
	}
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

func validateAction(path string, action ActionConfig) []string {
	var errs []string
	add := map[string]struct{}{}
	for _, label := range action.LabelsAdd {
		if _, exists := add[label]; exists {
			errs = append(errs, fmt.Sprintf("%s.labels_add contains duplicate %q", path, label))
		}
		add[label] = struct{}{}
	}
	remove := map[string]struct{}{}
	for _, label := range action.LabelsRemove {
		if _, exists := remove[label]; exists {
			errs = append(errs, fmt.Sprintf("%s.labels_remove contains duplicate %q", path, label))
		}
		remove[label] = struct{}{}
	}

	var conflicts []string
	for label := range add {
		if _, exists := remove[label]; exists {
			conflicts = append(conflicts, label)
		}
	}
	sort.Strings(conflicts)
	for _, label := range conflicts {
		errs = append(errs, fmt.Sprintf("%s adds and removes label %q", path, label))
	}
	return errs
}
