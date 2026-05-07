package projection

import (
	"context"
	"errors"
	"strings"
	"testing"

	"issueq/internal/model"
)

type fakeGitHub struct {
	comments []model.IssueComment
	created  []string
	updated  []string
	labels   []string
	set      []string
	fail     bool
}

func (f *fakeGitHub) ListIssueComments(ctx context.Context, owner, repo string, number int) ([]model.IssueComment, error) {
	if f.fail {
		return nil, errors.New("boom")
	}
	return f.comments, nil
}
func (f *fakeGitHub) GetIssue(ctx context.Context, owner, repo string, number int) (model.IssueSnapshot, error) {
	return model.IssueSnapshot{Host: "github.com", Owner: owner, Repo: repo, Number: number, Labels: append([]string(nil), f.labels...)}, nil
}
func (f *fakeGitHub) SetLabels(ctx context.Context, owner, repo string, number int, labels []string) error {
	f.set = append([]string(nil), labels...)
	return nil
}
func (f *fakeGitHub) CreateComment(ctx context.Context, owner, repo string, number int, body string) error {
	f.created = append(f.created, body)
	return nil
}
func (f *fakeGitHub) UpdateComment(ctx context.Context, owner, repo string, commentID string, body string) error {
	f.updated = append(f.updated, commentID+":"+body)
	return nil
}

func projectionEvent() model.AutomationEvent {
	return model.AutomationEvent{EventKey: "pr-review:github.com/o/r:pr-12:head-a", Kind: "pr-review", RouteName: "pr-review", Status: model.AutomationEventStatusSucceeded, RepoHost: "github.com", Owner: "o", Repo: "r", TargetKind: "pull_request", TargetKey: "pr-12", TargetFingerprint: "head-a", ResultJSON: `{"schema":"issueq-agent-result/v1","decision":"merge_ready","summary_markdown":"ready","projection":{"comment":"managed","labels":["agent-merge-ready","agent-active"]}}`}
}

func TestProjectEventCreatesOrUpdatesManagedCommentAndAllowedLabels(t *testing.T) {
	ev := projectionEvent()
	gh := &fakeGitHub{}
	res, err := ProjectEvent(context.Background(), gh, ev)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Created || len(gh.created) != 1 || !strings.Contains(gh.created[0], MarkerPrefix+ev.EventKey) {
		t.Fatalf("create result=%#v comments=%#v", res, gh.created)
	}
	if strings.Join(gh.set, ",") != "agent-active,agent-merge-ready" {
		t.Fatalf("labels set=%#v", gh.set)
	}

	gh = &fakeGitHub{comments: []model.IssueComment{{ID: "c1", Body: MarkerPrefix + ev.EventKey + " -->\nold"}}}
	res, err = ProjectEvent(context.Background(), gh, ev)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Updated || len(gh.updated) != 1 || !strings.HasPrefix(gh.updated[0], "c1:") {
		t.Fatalf("update result=%#v updated=%#v", res, gh.updated)
	}
}

func TestProjectEventFailureDoesNotMutateEvent(t *testing.T) {
	ev := projectionEvent()
	before := ev
	_, err := ProjectEvent(context.Background(), &fakeGitHub{fail: true}, ev)
	if err == nil {
		t.Fatal("expected projection failure")
	}
	if ev != before {
		t.Fatalf("projection mutated event: before=%#v after=%#v", before, ev)
	}
}
