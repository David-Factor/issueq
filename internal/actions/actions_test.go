package actions

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"issueq/internal/config"
)

func TestMergeConcatenatesComments(t *testing.T) {
	got := Merge(config.ActionConfig{Comment: "base"}, ResultAction{Comment: "result"})
	if got.Comment != "base\n\nresult" {
		t.Fatalf("comment = %q", got.Comment)
	}
}

func TestMergeResultFileLabelOpsWinConflicts(t *testing.T) {
	got := Merge(config.ActionConfig{LabelsAdd: []string{"agent-review"}, LabelsRemove: []string{"agent-running"}}, ResultAction{LabelsAdd: []string{"agent-running"}, LabelsRemove: []string{"agent-review"}})
	if strings.Join(got.LabelsAdd, ",") != "agent-running" {
		t.Fatalf("labels add = %#v", got.LabelsAdd)
	}
	if strings.Join(got.LabelsRemove, ",") != "agent-review" {
		t.Fatalf("labels remove = %#v", got.LabelsRemove)
	}
}

func TestParseResultFileErrors(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, found, err := ParseResultFile(bad)
	if !found || err == nil || !strings.Contains(err.Error(), "parse result JSON") {
		t.Fatalf("found=%v err=%v", found, err)
	}
	enqueue := filepath.Join(dir, "enqueue.json")
	if err := os.WriteFile(enqueue, []byte(`{"enqueue": []}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, found, err = ParseResultFile(enqueue)
	if !found || err == nil || !strings.Contains(err.Error(), `unsupported result JSON field "enqueue"`) {
		t.Fatalf("found=%v err=%v", found, err)
	}
}
