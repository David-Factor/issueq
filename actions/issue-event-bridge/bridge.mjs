#!/usr/bin/env node

import fs from "node:fs";

const apiVersionHeaders = {
  Accept: "application/vnd.github+json",
  "X-GitHub-Api-Version": "2022-11-28",
};

function readInput(name, fallback = "") {
  const key = `INPUT_${name.toUpperCase().replaceAll("-", "_")}`;
  return process.env[key] ?? fallback;
}

function setOutput(name, value) {
  const outputPath = process.env.GITHUB_OUTPUT;
  if (!outputPath) {
    return;
  }
  fs.appendFileSync(outputPath, `${name}=${String(value).replaceAll("\n", "%0A")}\n`);
}

function parseList(value) {
  return value
    .split(/[\n,]/)
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function parseBoolean(value) {
  return ["1", "true", "yes", "on"].includes(value.trim().toLowerCase());
}

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function parsePositiveInteger(value, label) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${label} must be a positive integer; received ${value}`);
  }
  return parsed;
}

async function githubRequest(input) {
  const response = await fetch(`https://api.github.com${input.path}`, {
    method: input.method ?? "GET",
    headers: {
      ...apiVersionHeaders,
      Authorization: `Bearer ${input.token}`,
      ...(input.body ? { "Content-Type": "application/json" } : {}),
    },
    body: input.body ? JSON.stringify(input.body) : undefined,
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : null;
  if (!response.ok) {
    throw new Error(`GitHub API ${input.method ?? "GET"} ${input.path} failed: status=${response.status} body=${text.slice(0, 1000)}`);
  }
  return data;
}

async function githubPaginate(input) {
  const results = [];
  for (let page = 1; page <= 10; page += 1) {
    const separator = input.path.includes("?") ? "&" : "?";
    const pageResults = await githubRequest({
      token: input.token,
      path: `${input.path}${separator}per_page=100&page=${page}`,
    });
    if (!Array.isArray(pageResults) || pageResults.length === 0) {
      break;
    }
    results.push(...pageResults);
    if (pageResults.length < 100) {
      break;
    }
  }
  return results;
}

function loadEventPayload() {
  const eventPath = requireEnv("GITHUB_EVENT_PATH");
  return JSON.parse(fs.readFileSync(eventPath, "utf8"));
}

function sanitizeMarkerPart(value) {
  return String(value)
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^-+|-+$/g, "") || "unknown";
}

function buildRunUrl(repo, runId) {
  return `https://github.com/${repo.owner}/${repo.repo}/actions/runs/${runId}`;
}

function findIssueReference(text) {
  const match = text.match(/(?:fixes|fixed|close[sd]?|resolve[sd]?|related to)\s+#(\d+)/i);
  return match ? `#${match[1]}` : "unknown";
}

function inferIssueFromBranch(branch) {
  const match = branch.match(/(?:issueq\/)?issue[-/](\d+)/i);
  return match ? `#${match[1]}` : "unknown";
}

function branchIsGenerated(branch, prefixes) {
  return prefixes.some((prefix) => branch.startsWith(prefix));
}

async function findPullRequestForWorkflowRun(input) {
  const prs = input.workflowRun.pull_requests ?? [];
  const first = prs[0];
  if (first?.number) {
    return await githubRequest({
      token: input.token,
      path: `/repos/${input.repo.owner}/${input.repo.repo}/pulls/${first.number}`,
    });
  }
  const headSha = input.workflowRun.head_sha;
  if (!headSha) {
    return null;
  }
  const pulls = await githubRequest({
    token: input.token,
    path: `/repos/${input.repo.owner}/${input.repo.repo}/commits/${headSha}/pulls`,
  });
  return Array.isArray(pulls) && pulls[0] ? pulls[0] : null;
}

function buildCiFailureBridge(input) {
  const workflowRun = input.event.workflow_run;
  if (!workflowRun) {
    throw new Error("ci-failure requires a workflow_run event payload");
  }
  if (workflowRun.conclusion !== "failure") {
    throw new Error(`ci-failure bridge only handles failed runs; conclusion=${workflowRun.conclusion ?? "unknown"}`);
  }
  const workflowName = workflowRun.name || "workflow";
  const prNumber = input.pullRequest?.number ?? workflowRun.pull_requests?.[0]?.number ?? "unknown";
  const headBranch = input.pullRequest?.head?.ref ?? workflowRun.head_branch ?? "unknown";
  const headSha = input.pullRequest?.head?.sha ?? workflowRun.head_sha ?? "unknown";
  const baseBranch = input.pullRequest?.base?.ref ?? "unknown";
  const backingIssue = input.pullRequest
    ? findIssueReference(`${input.pullRequest.title ?? ""}\n${input.pullRequest.body ?? ""}`)
    : inferIssueFromBranch(headBranch);
  const inferredBackingIssue = backingIssue === "unknown" ? inferIssueFromBranch(headBranch) : backingIssue;
  const marker = `<!-- issueq-bridge:ci-failure:pr-${sanitizeMarkerPart(prNumber)}:workflow-${sanitizeMarkerPart(workflowName)} -->`;
  const title = `${input.titlePrefix}: PR #${prNumber}`;
  const generatedBranch = branchIsGenerated(headBranch, input.generatedBranchPrefixes);
  const body = `${marker}

PR: ${input.pullRequest?.html_url ?? "unknown"}
Base branch: ${baseBranch}
Head branch: ${headBranch}
Head SHA: ${headSha}
Workflow: ${workflowName}
Run: ${workflowRun.html_url ?? buildRunUrl(input.repo, workflowRun.id)}
Backing issue: ${inferredBackingIssue}
Bridge event: ci-failure
Bridge dry-run: ${input.dryRun ? "true" : "false"}
Generated branch: ${generatedBranch ? "true" : "false"}

Failure summary:
- Workflow run concluded with failure.
- Inspect the linked run for failing jobs and logs.

Agent instructions:
${input.agentInstructions}
`;
  return { marker, title, body, generatedBranch };
}

async function findExistingIssue(input) {
  const encoded = encodeURIComponent(`${input.marker} repo:${input.repo.owner}/${input.repo.repo} is:issue is:open in:body`);
  const search = await githubRequest({
    token: input.token,
    path: `/search/issues?q=${encoded}`,
  });
  const item = search.items?.find((entry) => entry.body?.includes(input.marker));
  return item ?? null;
}

function extractAttempt(body) {
  const match = body.match(/^Attempt:\s*(\d+)\s*$/m);
  if (!match) {
    return 0;
  }
  const parsed = Number.parseInt(match[1], 10);
  return Number.isInteger(parsed) ? parsed : 0;
}

function upsertAttempt(body, attempt) {
  if (/^Attempt:\s*\d+\s*$/m.test(body)) {
    return body.replace(/^Attempt:\s*\d+\s*$/m, `Attempt: ${attempt}`);
  }
  return body.replace(/(Bridge event: .+\n)/, `$1Attempt: ${attempt}\n`);
}

async function ensureLabels(input) {
  const labels = [...new Set(input.labels.filter(Boolean))];
  if (labels.length === 0) {
    return;
  }
  await githubRequest({
    token: input.token,
    method: "POST",
    path: `/repos/${input.repo.owner}/${input.repo.repo}/issues/${input.issueNumber}/labels`,
    body: { labels },
  });
}

async function run() {
  const token = readInput("github-token") || process.env.GITHUB_TOKEN;
  if (!token) {
    throw new Error("github-token input or GITHUB_TOKEN is required");
  }
  const eventKind = readInput("event-kind").trim();
  if (eventKind !== "ci-failure") {
    throw new Error(`unsupported event-kind: ${eventKind}`);
  }
  const [owner, repoName] = requireEnv("GITHUB_REPOSITORY").split("/");
  if (!owner || !repoName) {
    throw new Error("GITHUB_REPOSITORY must be OWNER/REPO");
  }
  const repo = { owner, repo: repoName };
  const event = loadEventPayload();
  const workflowRun = event.workflow_run;
  const pullRequest = await findPullRequestForWorkflowRun({ token, repo, workflowRun });
  const dryRun = parseBoolean(readInput("dry-run", "true"));
  const generatedBranchPrefixes = parseList(readInput("generated-branch-prefixes", "issueq/,agent/,codex/"));
  const bridge = buildCiFailureBridge({
    event,
    pullRequest,
    repo,
    dryRun,
    generatedBranchPrefixes,
    titlePrefix: readInput("issue-title-prefix", "CI failure"),
    agentInstructions: readInput("agent-instructions", "Please inspect the failing CI run and produce a concise diagnosis."),
  });
  const existing = await findExistingIssue({ token, repo, marker: bridge.marker });
  const previousAttempt = existing?.body ? extractAttempt(existing.body) : 0;
  const attempt = previousAttempt + 1;
  const body = upsertAttempt(bridge.body, attempt);
  const maxAttempts = parsePositiveInteger(readInput("max-attempts", "2"), "max-attempts");
  const routingLabels = parseList(readInput("routing-labels", "agent-ci-diagnose"));
  const readyLabel = readInput("ready-label", "agent-ready").trim();
  const humanLabels = parseList(readInput("human-labels", "agent-needs-human,manual-only"));
  const readyAllowed = !dryRun && !bridge.generatedBranch && attempt <= maxAttempts;
  const labels = readyAllowed
    ? [...routingLabels, readyLabel]
    : attempt > maxAttempts
      ? [...routingLabels, ...humanLabels]
      : routingLabels;

  let issue;
  if (existing) {
    issue = await githubRequest({
      token,
      method: "PATCH",
      path: `/repos/${repo.owner}/${repo.repo}/issues/${existing.number}`,
      body: { title: bridge.title, body },
    });
  } else {
    issue = await githubRequest({
      token,
      method: "POST",
      path: `/repos/${repo.owner}/${repo.repo}/issues`,
      body: { title: bridge.title, body, labels },
    });
  }
  if (existing) {
    await ensureLabels({ token, repo, issueNumber: issue.number, labels });
  }

  console.log(`${existing ? "Updated" : "Created"} bridge issue #${issue.number}: ${issue.html_url}`);
  console.log(`marker=${bridge.marker}`);
  console.log(`attempt=${attempt} ready_applied=${readyAllowed}`);
  setOutput("issue-number", issue.number);
  setOutput("issue-url", issue.html_url);
  setOutput("marker", bridge.marker);
  setOutput("ready-applied", readyAllowed ? "true" : "false");
}

run().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
