---
name: autoresearch:plan
description: Interactive wizard to build Scope, Metric, Direction & Verify from a Goal
argument-hint: "[goal description]"
---

EXECUTE IMMEDIATELY — do not deliberate, do not ask clarifying questions before reading the protocol.

## Argument Parsing (do this FIRST)

Extract the goal
from $ARGUMENTS. The user may provide extensive context — treat the entire text as goal context. Look for
`Goal:` keyword; if absent, the full $ARGUMENTS text IS the goal.

## Execution

1. Read the plan workflow: `.claude/skills/autoresearch/references/plan-workflow.md`
2. Execute the 7-step planning wizard

Stream all output live — never run in background.
