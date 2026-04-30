package github

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRESTClientParsesIssuesAndSendsAuthorization(t *testing.T) {
	const token = "secret-token"
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if r.URL.Path != "/repos/example-org/example-repo/issues" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.URL.Query().Get("state") != "open" {
			t.Fatalf("state query = %q", r.URL.Query().Get("state"))
		}
		body := "body text"
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"node_id":    "node-1",
			"number":     1,
			"title":      "Title",
			"body":       body,
			"state":      "open",
			"updated_at": "2026-01-01T00:00:00Z",
			"labels":     []map[string]string{{"name": "agent-ready"}},
		}})
	}))
	defer server.Close()

	client, err := NewRESTClientWithBaseURL("github.com", server.URL, token, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	issues, err := client.ListOpenIssues(context.Background(), "example-org", "example-repo")
	if err != nil {
		t.Fatalf("ListOpenIssues() error = %v", err)
	}
	if gotAuth != "Bearer "+token {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if len(issues) != 1 {
		t.Fatalf("issues len = %d", len(issues))
	}
	issue := issues[0]
	if issue.IssueKey != "github.com/example-org/example-repo#1" || issue.NodeID != "node-1" || issue.Title != "Title" || issue.Body != "body text" || issue.State != "open" {
		t.Fatalf("issue = %#v", issue)
	}
	if len(issue.Labels) != 1 || issue.Labels[0] != "agent-ready" {
		t.Fatalf("labels = %#v", issue.Labels)
	}
	if !issue.GitHubUpdatedAt.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("updated = %s", issue.GitHubUpdatedAt)
	}
}

func TestRESTClientSkipsPullRequests(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"number":       1,
			"title":        "PR",
			"state":        "open",
			"updated_at":   "2026-01-01T00:00:00Z",
			"pull_request": map[string]any{},
		}})
	}))
	defer server.Close()
	client, err := NewRESTClientWithBaseURL("github.com", server.URL, "", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	issues, err := client.ListOpenIssues(context.Background(), "example-org", "example-repo")
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Fatalf("issues = %#v", issues)
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
	_, err = client.ListOpenIssues(context.Background(), "example-org", "example-repo")
	if err == nil {
		t.Fatal("ListOpenIssues() error = nil")
	}
	if strings.Contains(err.Error(), token) {
		t.Fatalf("error leaked token: %v", err)
	}
	if !strings.Contains(err.Error(), "[REDACTED]") {
		t.Fatalf("error missing redaction: %v", err)
	}
}

func TestIssueURLPath(t *testing.T) {
	if got := IssueURLPath("o/r", "repo name", 12); got != "/repos/o%2Fr/repo%20name/issues/12" {
		t.Fatalf("IssueURLPath() = %q", got)
	}
}
