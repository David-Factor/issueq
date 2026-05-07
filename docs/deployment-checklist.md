# IssueQ event deployment checklist

- Stop any previous IssueQ service before cutover.
- Use a fresh live SQLite DB for event rollout.
- Install the hard-cutover binary and verify `issueq --help` does not expose
  `--mode`, `poll`, `route`, or `dispatch`.
- Validate config with `issueq --config issueq.yaml config-check`.
- Start the trusted local event ingestion timer.
- Run one supervised report-only event with `issueq --config issueq.yaml once`.
- Project the event with `issueq --config issueq.yaml project <event-key>`.
- After local validation, start `issueq@<instance>.service` for unattended event
  scheduling.

Bridge issues, route labels, and legacy handoff comments are not migration
inputs. Keep any old state only as an external rollback/debug snapshot.
