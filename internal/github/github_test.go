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
	if comments[0].ID != "comment-node-99" || comments[0].IssueKey != "github.com/example-org/example-repo#12" || comments[0].Body != "```json\n{}\n```" {
		t.Fatalf("comment = %#v", comments[0])
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

func TestRESTClientEscapesPathSegmentsOnce(t *testing.T) {
	var got []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.URL.EscapedPath())
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewRESTClientWithBaseURL("github.com", server.URL, "", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.RemoveLabels(context.Background(), "o/r", "repo name", 12, []string{"needs info", "area/foo"}); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"/repos/o%2Fr/repo%20name/issues/12/labels/needs%20info",
		"/repos/o%2Fr/repo%20name/issues/12/labels/area%2Ffoo",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("paths = %#v, want %#v", got, want)
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
	if err := client.SetLabels(context.Background(), "o/r", "repo name", 12, []string{"agent-ready", "agent-route-pr-fix"}); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/repos/o%2Fr/repo%20name/issues/12/labels" {
		t.Fatalf("path = %q", gotPath)
	}
	if strings.Join(gotBody["labels"], ",") != "agent-ready,agent-route-pr-fix" {
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
