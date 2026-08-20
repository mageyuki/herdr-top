# Controller label provenance for task subjects

## Status

Accepted

## Date

2026-08-19

## Context

Controller events can provide short labels for display on Task Runs. These labels have historically been operator-provided text, while provider-derived prompt text, response text, and tool arguments and results are excluded from persistence and display.

Claude Code's `TaskCreated` hook includes `task_subject`, the agent-authored one-line name of the task. Forwarding that subject gives an announced Task Run a meaningful display name, but it requires an explicit, narrow exception to the rule that agent-generated content does not enter Controller labels.

## Decision

A Controller-supplied `label` may carry the agent-authored task subject: the task's one-line name. This is the only agent-generated content permitted in a Controller label. No prompt, response, description, task description, teammate name, team name, assistant message, tool input, tool result, or other agent-generated value is authorized by this decision, and the decision does not widen the provenance permitted for `reason` or any other field.

The task subject remains subject to the existing Controller label sanitization on ingest: a 256-byte cap, control-character escaping, and UTF-8-safe truncation. Hook adapters deserialize only the structural fields needed for mapping; content-bearing fields outside this exception are ignored rather than forwarded.

## Alternatives considered

The alternative was to emit the task event with no label. That would preserve the previous provenance boundary without an exception, but the Task Run would display only its structural identity instead of the task's one-line name.

## Consequences

Task Runs created from supported hooks can display their one-line task subject wherever Controller labels are shown. Reviewers and future adapters have a precise provenance boundary to enforce: the task subject is allowed, while every other agent-generated value remains excluded. The existing sanitization continues to bound the displayed and persisted label.
