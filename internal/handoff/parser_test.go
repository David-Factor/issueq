package handoff

import (
	"strings"
	"testing"
	"time"
)

func TestParseCommentExtractsValidHandoff(t *testing.T) {
	created := time.Date(2026, 5, 6, 1, 2, 3, 0, time.UTC)
	result := ParseComment("github.com/o/r#191", "prefix\n```issueq-handoff\n"+validPayload()+"\n```\nsuffix", created)
	if len(result.Diagnostics) != 0 {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Handoffs) != 1 {
		t.Fatalf("handoffs len = %d, want 1", len(result.Handoffs))
	}
	h := result.Handoffs[0]
	if h.ID == "" || !strings.HasPrefix(h.ID, "hnd_") {
		t.Fatalf("id = %q", h.ID)
	}
	if h.IssueKey != "github.com/o/r#191" || h.RouteName != "bug-triage" || h.Decision != "reproducible" || h.NextRoute != "bug-fix-pr" {
		t.Fatalf("handoff fields = %#v", h)
	}
	if h.SourceKind != "github_issue" || h.SourceKey != "#191" || h.SourceFingerprint != "abc123" {
		t.Fatalf("source fields = %#v", h)
	}
	if h.TargetKind != "bug_issue" || h.TargetKey != "#191" {
		t.Fatalf("target fields = %#v", h)
	}
	if !h.CreatedAt.Equal(created) {
		t.Fatalf("created_at = %s", h.CreatedAt)
	}
}

func TestParseCommentIgnoresNonHandoffComments(t *testing.T) {
	result := ParseComment("issue", "hello\n```issueq-handoff\n{\"schema\":\"else\"}\n```", time.Now())
	if len(result.Handoffs) != 0 || len(result.Diagnostics) != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestParseCommentIgnoresOrdinaryJSONFence(t *testing.T) {
	result := ParseComment("issue", "```json\n"+validPayload()+"\n```", time.Now())
	if len(result.Handoffs) != 0 || len(result.Diagnostics) != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestParseCommentIgnoresUnlabeledFence(t *testing.T) {
	result := ParseComment("issue", "```\n"+validPayload()+"\n```", time.Now())
	if len(result.Handoffs) != 0 || len(result.Diagnostics) != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestParseCommentIgnoresLegacyHTMLMarker(t *testing.T) {
	result := ParseComment("issue", "<!-- issueq-handoff:v1 {\"schema\":\"issueq-handoff/v1\",\"route\":\"bug-triage\",\"decision\":\"accepted\"} -->", time.Now())
	if len(result.Handoffs) != 0 || len(result.Diagnostics) != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestParseCommentMalformedHandoffDiagnostic(t *testing.T) {
	result := ParseComment("issue", "```issueq-handoff\n{\"schema\":\"issueq-handoff/v1\",\n```", time.Now())
	if len(result.Handoffs) != 0 {
		t.Fatalf("handoffs = %#v", result.Handoffs)
	}
	if len(result.Diagnostics) != 1 {
		t.Fatalf("diagnostics len = %d, want 1", len(result.Diagnostics))
	}
}

func TestParseCommentPreservesUnknownPayloadFields(t *testing.T) {
	result := ParseComment("issue", "```issueq-handoff\n"+validPayload()+"\n```", time.Now())
	if len(result.Handoffs) != 1 {
		t.Fatalf("handoffs len = %d", len(result.Handoffs))
	}
	if !strings.Contains(result.Handoffs[0].PayloadJSON, "unknown_field") {
		t.Fatalf("payload_json = %s", result.Handoffs[0].PayloadJSON)
	}
}

func TestParseCommentIDIsStable(t *testing.T) {
	first := ParseComment("issue", "```issueq-handoff\n"+validPayload()+"\n```", time.Now()).Handoffs[0]
	second := ParseComment("issue", "```issueq-handoff\n"+validPayload()+"\n```", time.Now().Add(time.Hour)).Handoffs[0]
	if first.ID != second.ID {
		t.Fatalf("ids differ: %q %q", first.ID, second.ID)
	}
}

func validPayload() string {
	return `{
  "schema": "issueq-handoff/v1",
  "schema_version": "1",
  "route": "bug-triage",
  "decision": "reproducible",
  "next_route": "bug-fix-pr",
  "source": {"kind": "github_issue", "issue_number": 191, "body_sha256": "abc123"},
  "target": {"kind": "bug_issue", "issue_number": 191},
  "unknown_field": {"kept": true}
}`
}
