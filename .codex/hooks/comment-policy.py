#!/usr/bin/env python3
"""Advisory PostToolUse hook: flag comment-policy violations in Go edits.

Never blocks. On a Write/Edit to a *.go file it scans the ADDED text's comment
lines for the narration the project keeps having to strip (scope/limitation,
incident/reproduction, task references) and, if any are found, returns
additionalContext so the model can self-correct in a follow-up edit.

See .claude/rules/comments.md for the policy this enforces.
"""
import json
import re
import sys

# (label, regex) — case-insensitive. Targeted at the recurring offenders, kept
# conservative to avoid false-positive noise on legitimate why-comments.
PATTERNS = [
    ("scope/limitation narration (→ PR body)",
     re.compile(r"forward[- ](only|prevention)|safety[- ]?net|cannot repair|snapshot unwind|\bNOTE:\s", re.I)),
    ("incident/reproduction narration (→ PR body)",
     re.compile(r"\bmainnet\b|\bblock[s]?\s+\d{6,9}\b|\b2[0-9]{7}\b|\bcalled\b.*\bblocks? later\b", re.I)),
    ("task/PR/issue reference (→ commit msg / PR; keep only genuine workaround links)",
     re.compile(r"#\d{3,6}\b|\bPR\s*#?\d{3,6}\b|as requested|in review")),
]


def added_comment_lines(tool_name, tool_input):
    if tool_name == "Write":
        text = tool_input.get("content", "")
    elif tool_name in ("Edit", "MultiEdit"):
        # new_string for Edit; concatenate edits for MultiEdit
        if "new_string" in tool_input:
            text = tool_input.get("new_string", "")
        else:
            text = "\n".join(e.get("new_string", "") for e in tool_input.get("edits", []))
    else:
        return []
    out = []
    for ln in text.splitlines():
        s = ln.strip()
        if s.startswith("//") or s.startswith("/*") or s.startswith("*"):
            out.append(s)
    return out


def main():
    try:
        data = json.load(sys.stdin)
    except Exception:
        sys.exit(0)  # never interfere

    tool_name = data.get("tool_name", "")
    tool_input = data.get("tool_input", {}) or {}
    path = tool_input.get("file_path", "")
    if not path.endswith(".go"):
        sys.exit(0)

    comments = added_comment_lines(tool_name, tool_input)
    if not comments:
        sys.exit(0)

    flagged = []
    for line in comments:
        for label, rx in PATTERNS:
            if rx.search(line):
                flagged.append((label, line))
                break

    if not flagged:
        sys.exit(0)

    msg = ["Comment-policy check (.claude/rules/comments.md) — review these added comment lines; "
           "scope/incident/task narration belongs in the commit message or PR body, not the code:"]
    for label, line in flagged[:12]:
        snippet = line if len(line) <= 100 else line[:97] + "..."
        msg.append(f"  - [{label}] {snippet}")
    msg.append("If a flagged line is a genuine why-comment (e.g. a workaround issue link), keep it.")

    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PostToolUse",
            "additionalContext": "\n".join(msg),
        }
    }))
    sys.exit(0)


if __name__ == "__main__":
    main()
