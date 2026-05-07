package github

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRESTClientListsIssueComments(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/example-org/example-repo/issues/12/comments" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.URL.Query().Get("per_page") != "100" {
			t.Fatalf("per_page query = %q", r.URL.Query().Get("per_page"))
		}
		body := "```json\n{}\n```"
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"id":         99,
			"node_id":    "comment-node-99",
			"body":       body,
			"created_at": "2026-01-01T00:00:00Z",
			"updated_at": "2026-01-01T00:01:00Z",
		}})
	}))
	defer server.Close()
	client, err := NewRESTClientWithBaseURL("github.com", server.URL, "", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	comments, err := client.ListIssueComments(context.Background(), "example-org", "example-repo", 12)
	if err != nil {
		t.Fatalf("ListIssueComments() error = %v", err)
	}
	if len(comments) != 1 {
		t.Fatalf("comments len = %d", len(comments))
	}
	if comments[0].ID != "99" || comments[0].IssueKey != "github.com/example-org/example-repo#12" || comments[0].Body != "```json\n{}\n```" {
		t.Fatalf("comment = %#v", comments[0])
	}
}

func TestRESTClientUpdatesComment(t *testing.T) {
	var gotMethod, gotPath, gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		gotBody = body["body"]
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client, err := NewRESTClientWithBaseURL("github.com", server.URL, "", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.UpdateComment(context.Background(), "example-org", "example-repo", "comment-node-99", "updated"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPatch || gotPath != "/repos/example-org/example-repo/issues/comments/comment-node-99" || gotBody != "updated" {
		t.Fatalf("method=%s path=%s body=%q", gotMethod, gotPath, gotBody)
	}
}

func TestRESTClientRedactsTokenFromErrors(t *testing.T) {
	const token = "super-secret-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad token "+token, http.StatusUnauthorized)
	}))
	defer server.Close()
	client, err := NewRESTClientWithBaseURL("github.com", server.URL, token, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetIssue(context.Background(), "example-org", "example-repo", 12)
	if err == nil {
		t.Fatal("GetIssue() error = nil")
	}
	if strings.Contains(err.Error(), token) {
		t.Fatalf("error leaked token: %v", err)
	}
	if !strings.Contains(err.Error(), "[REDACTED]") {
		t.Fatalf("error missing redaction: %v", err)
	}
}

func TestRESTClientSetsLabelsWithEscapedPath(t *testing.T) {
	var gotPath string
	var gotBody map[string][]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, err := NewRESTClientWithBaseURL("github.com", server.URL, "", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SetLabels(context.Background(), "o/r", "repo name", 12, []string{"agent-active", "agent-failed"}); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/repos/o%2Fr/repo%20name/issues/12/labels" {
		t.Fatalf("path = %q", gotPath)
	}
	if strings.Join(gotBody["labels"], ",") != "agent-active,agent-failed" {
		t.Fatalf("body = %#v", gotBody)
	}
}

func TestNewRESTClientUsesDefaultHTTPTimeout(t *testing.T) {
	client, err := NewRESTClient("github.com", "")
	if err != nil {
		t.Fatal(err)
	}
	if client.http == nil || client.http.Timeout != defaultHTTPClientTimeout {
		t.Fatalf("timeout = %v, want %v", client.http.Timeout, defaultHTTPClientTimeout)
	}
}

func TestIssueURLPath(t *testing.T) {
	if got := IssueURLPath("o/r", "repo name", 12); got != "/repos/o%2Fr/repo%20name/issues/12" {
		t.Fatalf("IssueURLPath() = %q", got)
	}
}
