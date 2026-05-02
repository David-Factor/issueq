# Security policy

## Supported versions

IssueQ is currently early public-preview software. Security fixes are made on `main` until formal releases are cut.

## Reporting vulnerabilities

Please report suspected vulnerabilities privately to the repository owner instead of opening a public issue.

If GitHub private vulnerability reporting is enabled for the repository, use that. Otherwise contact the maintainer through their GitHub profile and include:

- affected commit or version;
- impact and reproduction steps;
- whether secrets, tokens, logs, or job artifacts may be exposed;
- suggested mitigation, if known.

## Operational security model

IssueQ is not a sandbox. It runs configured local commands on the daemon host and writes queue/job state to SQLite.

Operators should:

- use a dedicated service user;
- use least-privilege GitHub tokens;
- keep env files, tokens, SQLite DBs, workdirs, and logs out of Git;
- avoid passing secrets to jobs unless required;
- avoid executing untrusted fork/PR code without an external sandbox;
- back up SQLite before manual repair;
- review systemd hardening before granting jobs broader filesystem access.
