// Package github contains the fakeable GitHub client boundary used by issueq.
package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"issueq/internal/model"
)

const (
	defaultAPIBaseURL        = "https://api.github.com"
	defaultHTTPClientTimeout = 30 * time.Second
)

type Client interface {
	ListOpenIssues(ctx context.Context, owner, repo string) ([]model.IssueSnapshot, error)
	GetIssue(ctx context.Context, owner, repo string, number int) (model.IssueSnapshot, error)
	AddLabels(ctx context.Context, owner, repo string, number int, labels []string) error
	RemoveLabels(ctx context.Context, owner, repo string, number int, labels []string) error
	CreateComment(ctx context.Context, owner, repo string, number int, body string) error
}

type RESTClient struct {
	host    string
	baseURL *url.URL
	token   string
	http    *http.Client
}

func NewRESTClient(host, token string) (*RESTClient, error) {
	base := defaultAPIBaseURL
	if host != "" && host != "github.com" {
		base = "https://" + host + "/api/v3"
	}
	return NewRESTClientWithBaseURL(host, base, token, nil)
}

func NewRESTClientWithBaseURL(host, baseURL, token string, httpClient *http.Client) (*RESTClient, error) {
	if strings.TrimSpace(host) == "" {
		host = "github.com"
	}
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("parse GitHub API base URL: %w", err)
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultHTTPClientTimeout}
	}
	return &RESTClient{host: host, baseURL: parsed, token: token, http: httpClient}, nil
}

func githubPath(path string, args ...any) string {
	values := make([]any, len(args))
	for i, arg := range args {
		switch value := arg.(type) {
		case string:
			values[i] = url.PathEscape(value)
		default:
			values[i] = value
		}
	}
	return fmt.Sprintf(path, values...)
}

func (c *RESTClient) ListOpenIssues(ctx context.Context, owner, repo string) ([]model.IssueSnapshot, error) {
	var all []model.IssueSnapshot
	for page := 1; ; page++ {
		path := githubPath("/repos/%s/%s/issues?state=open&per_page=100&page=%d", owner, repo, page)
		var raw []restIssue
		if err := c.do(ctx, http.MethodGet, path, nil, &raw); err != nil {
			return nil, err
		}
		for _, issue := range raw {
			if issue.PullRequest != nil {
				continue
			}
			all = append(all, issue.snapshot(c.host, owner, repo))
		}
		if len(raw) < 100 {
			break
		}
	}
	return all, nil
}

func (c *RESTClient) GetIssue(ctx context.Context, owner, repo string, number int) (model.IssueSnapshot, error) {
	path := githubPath("/repos/%s/%s/issues/%d", owner, repo, number)
	var raw restIssue
	if err := c.do(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return model.IssueSnapshot{}, err
	}
	return raw.snapshot(c.host, owner, repo), nil
}

func (c *RESTClient) AddLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	path := githubPath("/repos/%s/%s/issues/%d/labels", owner, repo, number)
	return c.do(ctx, http.MethodPost, path, map[string][]string{"labels": labels}, nil)
}

func (c *RESTClient) RemoveLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	for _, label := range labels {
		path := githubPath("/repos/%s/%s/issues/%d/labels/%s", owner, repo, number, label)
		if err := c.do(ctx, http.MethodDelete, path, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

func (c *RESTClient) CreateComment(ctx context.Context, owner, repo string, number int, body string) error {
	path := githubPath("/repos/%s/%s/issues/%d/comments", owner, repo, number)
	return c.do(ctx, http.MethodPost, path, map[string]string{"body": body}, nil)
}

func (c *RESTClient) do(ctx context.Context, method, path string, input, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("marshal GitHub request: %w", err)
		}
		body = bytes.NewReader(data)
	}

	reqURL := c.baseURL.String() + path
	req, err := http.NewRequestWithContext(ctx, method, reqURL, body)
	if err != nil {
		return fmt.Errorf("create GitHub request: %w", err)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := c.http.Do(req)
	if err != nil {
		return errors.New(RedactSecrets(fmt.Sprintf("GitHub API request failed: %v", err), c.token))
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		msg := RedactSecrets(string(data), c.token)
		return fmt.Errorf("GitHub API %s %s failed: status %d: %s", method, pathForError(path), resp.StatusCode, msg)
	}
	if output == nil || resp.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(output); err != nil {
		return fmt.Errorf("decode GitHub response: %w", err)
	}
	return nil
}

func pathForError(path string) string {
	if idx := strings.IndexByte(path, '?'); idx >= 0 {
		return path[:idx]
	}
	return path
}

func RedactSecrets(message string, secrets ...string) string {
	redacted := message
	for _, secret := range secrets {
		if secret == "" {
			continue
		}
		redacted = strings.ReplaceAll(redacted, secret, "[REDACTED]")
	}
	return redacted
}

type restIssue struct {
	NodeID      string           `json:"node_id"`
	Number      int              `json:"number"`
	Title       string           `json:"title"`
	Body        *string          `json:"body"`
	State       string           `json:"state"`
	UpdatedAt   time.Time        `json:"updated_at"`
	Labels      []restLabel      `json:"labels"`
	PullRequest *json.RawMessage `json:"pull_request"`
}

type restLabel struct {
	Name string `json:"name"`
}

func (i restIssue) snapshot(host, owner, repo string) model.IssueSnapshot {
	body := ""
	if i.Body != nil {
		body = *i.Body
	}
	labels := make([]string, 0, len(i.Labels))
	for _, label := range i.Labels {
		labels = append(labels, label.Name)
	}
	return model.IssueSnapshot{
		IssueKey:        model.IssueKey(host, owner, repo, i.Number),
		NodeID:          i.NodeID,
		Host:            host,
		Owner:           owner,
		Repo:            repo,
		Number:          i.Number,
		Title:           i.Title,
		Body:            body,
		Labels:          labels,
		State:           i.State,
		GitHubUpdatedAt: i.UpdatedAt,
	}
}

func IssueURLPath(owner, repo string, number int) string {
	return "/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(repo) + "/issues/" + strconv.Itoa(number)
}
