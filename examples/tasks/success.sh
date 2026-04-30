#!/bin/sh
set -eu
context_path="$1"
result_path="$2"
echo "issueq sample task"
echo "context: $context_path"
printf '{"comment":"Sample task completed.","labels_add":["agent-review"],"labels_remove":["agent-running"]}\n' > "$result_path"
