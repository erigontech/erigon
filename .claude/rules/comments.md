# Comment Policy

Default to **no comment**. Clear names and small functions should carry the meaning. Write a comment only when the code genuinely can't tell the reader *why*.

## Write a comment only for

- Workarounds for bugs in deps/runtime/other code (link the issue/commit).
- Non-obvious invariants or constraints not enforced by types.
- Surprising edge cases easy to miss when reading.
- Performance choices where the straightforward code would be wrong.

## Never put in code (→ goes in the commit message or PR body)

- **Scope / limitation / "honest" narration** — "forward-only", "safety net", "cannot repair X", "needs a snapshot unwind", "NOTE: this only…". That's PR-description material.
- **Incident / reproduction narration** — mainnet block numbers, "deployed via X, called N blocks later at M", post-mortem storytelling.
- **Task references** — PR numbers, issue numbers (except a genuine workaround link), "added for the X flow", "as requested in review".
- **Restating the code** — `// increment counter`, narrating what the next line obviously does.
- **The same rationale repeated at multiple sites** — state the *why* once at the canonical place (the field/type/function it belongs to); use terse pointers elsewhere, not the full paragraph again.

## When a comment is warranted

Keep it concise and *why*-focused, but err on the side of clarity: cut what only adds length, keep what saves the reader a wrong guess. If a reader could delete it without losing information, it shouldn't exist. Two clear sentences beat both a cryptic one-liner and a six-line essay.

Test docstrings get slightly more latitude (explaining a non-obvious scenario the test pins), but the same rules apply — no incident/scope/task narration.
