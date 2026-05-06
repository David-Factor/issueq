package handoff

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"issueq/internal/model"
)

const SchemaV1 = "issueq-handoff/v1"

var fencedBlockRE = regexp.MustCompile("(?s)```issueq-handoff[ \\t]*\\n(.*?)\\n```")

type Diagnostic struct {
	Reason string
	Err    error
}

type ParseResult struct {
	Handoffs    []model.Handoff
	Diagnostics []Diagnostic
}

func ParseComment(issueKey, body string, createdAt time.Time) ParseResult {
	var result ParseResult
	for _, match := range fencedBlockRE.FindAllStringSubmatch(body, -1) {
		payload := strings.TrimSpace(match[1])
		if !strings.Contains(payload, SchemaV1) {
			continue
		}
		handoff, err := parsePayload(issueKey, payload, createdAt)
		if err != nil {
			result.Diagnostics = append(result.Diagnostics, Diagnostic{Reason: "invalid_handoff", Err: err})
			continue
		}
		result.Handoffs = append(result.Handoffs, handoff)
	}
	return result
}

func parsePayload(issueKey, payload string, createdAt time.Time) (model.Handoff, error) {
	var env envelope
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&env); err != nil {
		return model.Handoff{}, fmt.Errorf("parse handoff JSON: %w", err)
	}
	if env.Schema != SchemaV1 {
		return model.Handoff{}, fmt.Errorf("unsupported handoff schema %q", env.Schema)
	}
	if strings.TrimSpace(env.Route) == "" {
		return model.Handoff{}, fmt.Errorf("handoff route is required")
	}
	if strings.TrimSpace(env.Decision) == "" {
		return model.Handoff{}, fmt.Errorf("handoff decision is required")
	}
	var raw any
	if err := json.Unmarshal([]byte(payload), &raw); err != nil {
		return model.Handoff{}, fmt.Errorf("parse handoff JSON for canonical payload: %w", err)
	}
	payloadBytes, err := json.Marshal(raw)
	if err != nil {
		return model.Handoff{}, fmt.Errorf("canonicalize handoff payload: %w", err)
	}
	payloadJSON := string(payloadBytes)
	return model.Handoff{
		ID:                stableID(issueKey, payloadJSON),
		IssueKey:          issueKey,
		RouteName:         env.Route,
		Decision:          env.Decision,
		NextRoute:         env.NextRoute,
		SourceKind:        env.Source.Kind,
		SourceKey:         firstNonEmpty(env.Source.Key, env.Source.IssueKey, issueNumberKey(env.Source.IssueNumber)),
		SourceFingerprint: firstNonEmpty(env.Source.Fingerprint, env.Source.SourceFingerprint, env.Source.BodySHA256, env.Source.UpdatedAt),
		TargetKind:        env.Target.Kind,
		TargetKey:         firstNonEmpty(env.Target.Key, env.Target.IssueKey, issueNumberKey(env.Target.IssueNumber)),
		PayloadJSON:       payload,
		CreatedAt:         createdAt.UTC(),
	}, nil
}

func stableID(issueKey, payloadJSON string) string {
	h := sha256.Sum256([]byte(issueKey + "\x00" + payloadJSON))
	return "hnd_" + hex.EncodeToString(h[:])
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func issueNumberKey(number int) string {
	if number == 0 {
		return ""
	}
	return fmt.Sprintf("#%d", number)
}

type envelope struct {
	Schema        string        `json:"schema"`
	SchemaVersion string        `json:"schema_version"`
	Route         string        `json:"route"`
	Decision      string        `json:"decision"`
	NextRoute     string        `json:"next_route"`
	Source        endpointBlock `json:"source"`
	Target        endpointBlock `json:"target"`
}

type endpointBlock struct {
	Kind              string `json:"kind"`
	Key               string `json:"key"`
	IssueKey          string `json:"issue_key"`
	IssueNumber       int    `json:"issue_number"`
	Fingerprint       string `json:"fingerprint"`
	SourceFingerprint string `json:"source_fingerprint"`
	BodySHA256        string `json:"body_sha256"`
	UpdatedAt         string `json:"updated_at"`
}
