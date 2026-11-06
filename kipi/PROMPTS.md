# PROMPTS.md — AI Prompt Log

This file records every prompt sent to AI tools during this project, in chronological order.

---

**Tool:** Antigravity (Claude Sonnet)  
**Time:** 2026-11-06 ~15:30 IST

> "read the nvm pdf and start building cli for this project use commands strictly given in kill it and prove it-1 pdf and build the solution"

---

**Tool:** Antigravity (Claude Sonnet)  
**Time:** 2026-11-06 ~15:42 IST

> "continue"

(The agent resumed after a server restart and built the full project: engine, CLI, torture tool, unit tests, and all five required documents.)

---

## Notes

The AI was used to:
1. Extract and understand the PDF specifications (nvm.pdf = project structure guide, Kill It and Prove It-1.pdf = problem statement).
2. Generate all Go source code for the engine, CLI wrapper, and torture tool.
3. Write all required documentation (README, ARCHITECTURE, BREAKING, DESIGN-EXTENSION).

The team reviewed and can explain every part of the code. Key design decisions made by the team:
- **Append-only log with in-memory hash index**: simplest design that satisfies all invariants.
- **Single `WriteAt` + single `Sync` per batch**: minimises syscalls while guaranteeing atomicity.
- **Linear recovery scan**: simple and correct; meets the 2-second target at 1M keys.
- **Windows-specific platform layer**: `LockFileEx` instead of `flock`, close-rename-reopen instead of POSIX rename-on-open-fd.

