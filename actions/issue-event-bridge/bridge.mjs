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
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      throw new Error(`GitHub API ${input.method ?? "GET"} ${input.path} returned invalid JSON: status=${response.status} body=${text.slice(0, 1000)}`);
    }
  }
  if (!response.ok) {
    throw new Error(`GitHub API ${input.method ?? "GET"} ${input.path} failed: status=${response.status} body=${text.slice(0, 1000)}`);
  }
  return data;
}

function loadEventPayload() {
  const eventPath = process.env.GITHUB_EVENT_PATH;
  if (!eventPath) {
    return {};
  }
  return JSON.parse(fs.readFileSync(eventPath, "utf8"));
}

function sanitizeMarkerPart(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^-+|-+$/g, "") || "unknown";
}

function buildRunUrl(repo, runId) {
  return runId ? `https://github.com/${repo.owner}/${repo.repo}/actions/runs/${runId}` : "unknown";
}

function findIssueReference(text) {
  const match = String(text ?? "").match(/(?:fixes|fixed|close[sd]?|resolve[sd]?|related to)\s+#(\d+)/i);
  return match ? `#${match[1]}` : "unknown";
}

function inferIssueFromBranch(branch) {
  const match = String(branch ?? "").match(/(?:issueq\/)?issue[-/](\d+)/i);
  return match ? `#${match[1]}` : "unknown";
}

function branchIsGenerated(branch, prefixes) {
  if (!branch) {
    return false;
  }
  return prefixes.some((prefix) => branch.startsWith(prefix));
}

function readPath(input, path) {
  return path.split(".").reduce((value, part) => {
    if (value === undefined || value === null) {
      return undefined;
    }
    if (Array.isArray(value)) {
      const index = Number.parseInt(part, 10);
      return Number.isInteger(index) ? value[index] : undefined;
    }
    if (typeof value !== "object") {
      return undefined;
    }
    return value[part];
  }, input);
}

function flattenContext(input) {
  const output = {};
  function visit(prefix, value) {
    if (value === undefined || value === null) {
      output[prefix] = "";
      return;
    }
    if (typeof value !== "object") {
      output[prefix] = String(value);
      return;
    }
    if (Array.isArray(value)) {
      output[prefix] = JSON.stringify(value);
      value.forEach((entry, index) => visit(`${prefix}.${index}`, entry));
      return;
    }
    output[prefix] = JSON.stringify(value);
    for (const [key, entry] of Object.entries(value)) {
      visit(prefix ? `${prefix}.${key}` : key, entry);
    }
  }
  visit("", input);
  delete output[""];
  return output;
}

function renderTemplate(template, context) {
  const flattened = flattenContext(context);
  return template.replace(/{{\s*([a-zA-Z0-9_.-]+)(?:\|([a-zA-Z0-9_-]+))?\s*}}/g, (_match, key, transform) => {
    const value = flattened[key] ?? "";
    if (transform === "slug") {
      return sanitizeMarkerPart(value);
    }
    if (transform) {
      throw new Error(`unsupported template transform: ${transform}`);
    }
    return value;
  });
}

async function findPullRequestForWorkflowRun(input) {
  const workflowRun = input.event.workflow_run;
  if (!workflowRun) {
    return null;
  }
  const first = workflowRun.pull_requests?.[0];
  if (first?.number) {
    return await githubRequest({
      token: input.token,
      path: `/repos/${input.repo.owner}/${input.repo.repo}/pulls/${first.number}`,
    });
  }
  const headSha = workflowRun.head_sha;
  if (!headSha) {
    return null;
  }
  const pulls = await githubRequest({
    token: input.token,
    path: `/repos/${input.repo.owner}/${input.repo.repo}/commits/${headSha}/pulls`,
  });
  return Array.isArray(pulls) && pulls[0] ? pulls[0] : null;
}

function buildDefaultWorkflowRunContext(input) {
  const workflowRun = input.event.workflow_run ?? {};
  const workflowName = workflowRun.name || "workflow";
  const prNumber = input.pullRequest?.number ?? workflowRun.pull_requests?.[0]?.number ?? "unknown";
  const headBranch = input.pullRequest?.head?.ref ?? workflowRun.head_branch ?? "unknown";
  const headSha = input.pullRequest?.head?.sha ?? workflowRun.head_sha ?? "unknown";
  const baseBranch = input.pullRequest?.base?.ref ?? "unknown";
  const backingIssueFromPr = input.pullRequest
    ? findIssueReference(`${input.pullRequest.title ?? ""}\n${input.pullRequest.body ?? ""}`)
    : "unknown";
  const backingIssue = backingIssueFromPr === "unknown" ? inferIssueFromBranch(headBranch) : backingIssueFromPr;
  return {
    event: input.event,
    repository: `${input.repo.owner}/${input.repo.repo}`,
    workflow: {
      name: workflowName,
      conclusion: workflowRun.conclusion ?? "unknown",
      run_url: workflowRun.html_url ?? buildRunUrl(input.repo, workflowRun.id),
      run_id: workflowRun.id ?? "unknown",
    },
    pr: {
      number: prNumber,
      url: input.pullRequest?.html_url ?? "unknown",
      title: input.pullRequest?.title ?? "unknown",
      base_branch: baseBranch,
      head_branch: headBranch,
      head_sha: headSha,
      backing_issue: backingIssue,
    },
  };
}

async function buildContext(input) {
  const explicit = readInput("context-json").trim();
  if (explicit) {
    return JSON.parse(explicit);
  }
  const pullRequest = await findPullRequestForWorkflowRun(input);
  return buildDefaultWorkflowRunContext({ ...input, pullRequest });
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
  return `${body.replace(/\s*$/, "")}\nAttempt: ${attempt}\n`;
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
  const [owner, repoName] = requireEnv("GITHUB_REPOSITORY").split("/");
  if (!owner || !repoName) {
    throw new Error("GITHUB_REPOSITORY must be OWNER/REPO");
  }
  const repo = { owner, repo: repoName };
  const event = loadEventPayload();
  const context = await buildContext({ token, repo, event });
  const marker = renderTemplate(readInput("marker"), context).trim();
  const title = renderTemplate(readInput("title"), context).trim();
  const bodyWithoutAttempt = renderTemplate(readInput("body"), context).trim();
  if (!marker || !title || !bodyWithoutAttempt) {
    throw new Error("marker, title, and body inputs must render to non-empty strings");
  }
  const existing = await findExistingIssue({ token, repo, marker });
  const previousAttempt = existing?.body ? extractAttempt(existing.body) : 0;
  const attempt = previousAttempt + 1;
  const body = upsertAttempt(bodyWithoutAttempt.includes(marker) ? bodyWithoutAttempt : `${marker}\n\n${bodyWithoutAttempt}`, attempt);
  const generatedBranch = renderTemplate(readInput("generated-branch"), context).trim();
  const generatedBranchPrefixes = parseList(readInput("generated-branch-prefixes", "issueq/,agent/,codex/"));
  const generated = branchIsGenerated(generatedBranch, generatedBranchPrefixes);
  const applyReady = parseBoolean(readInput("apply-ready", "false"));
  const maxAttempts = parsePositiveInteger(readInput("max-attempts", "2"), "max-attempts");
  const routingLabels = parseList(readInput("routing-labels"));
  const readyLabel = readInput("ready-label", "agent-ready").trim();
  const humanLabels = parseList(readInput("human-labels", "agent-needs-human,manual-only"));
  const readyAllowed = applyReady && !generated && attempt <= maxAttempts;
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
      body: { title, body },
    });
  } else {
    issue = await githubRequest({
      token,
      method: "POST",
      path: `/repos/${repo.owner}/${repo.repo}/issues`,
      body: { title, body, labels },
    });
  }
  if (existing) {
    await ensureLabels({ token, repo, issueNumber: issue.number, labels });
  }

  console.log(`${existing ? "Updated" : "Created"} bridge issue #${issue.number}: ${issue.html_url}`);
  console.log(`marker=${marker}`);
  console.log(`attempt=${attempt} ready_applied=${readyAllowed}`);
  setOutput("issue-number", issue.number);
  setOutput("issue-url", issue.html_url);
  setOutput("marker", marker);
  setOutput("ready-applied", readyAllowed ? "true" : "false");
  setOutput("attempt", attempt);
}

run().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
