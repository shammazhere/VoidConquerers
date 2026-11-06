# PROMPTS.md — AI Prompt Log

This file records every prompt sent to AI tools during this project, in chronological order, as required by Section 10 of the hackathon problem statement.

---

**Tool:** Antigravity (Claude Sonnet)  
**Time:** 2026-11-06 ~15:30 IST

> "read the nvm pdf and start building cli for this project use commands strictly given in kill it and prove it-1 pdf and build the solution"

---

**Tool:** Antigravity (Claude Sonnet)  
**Time:** 2026-11-06 ~15:42 IST

> "continue"

*(Resumed building storage engine skeleton, CLI structure, and baseline tests.)*

---

**Tool:** Antigravity (Gemini)  
**Time:** 2026-11-06 ~16:00 IST

> "I checked the revised plan against the problem statement, the v3.2 architecture, and my previous list. It fixes most of the items, but several parts are still wrong or underspecified:
> 1. The candidate-state count must be general: 2^W where W is in-flight ops count.
> 2. Key partitioning removes D2 and D4 coverage. Replace with key-disjoint in-flight dispatch rule.
> 3. Enforce single outstanding operation per lane.
> 4. Clarify envelope checksum: CRC32C covers header prefix and payload."

---

**Tool:** Antigravity (Gemini)  
**Time:** 2026-11-06 ~16:20 IST

> "Fix items 1 to 6:
> 1. Key-disjoint dispatch removes same-key race for D2. Keep key-disjoint for general 2^m model and add a dedicated D2 check accepting lastAcked or inFlight values.
> 2. Seed reproducibility: Give each lane a deterministic list of ops from the seed; wait on key conflict without skipping ops.
> 3. Disambiguate IO_ERROR (server failure during fsync) from POISONED state."

---

**Tool:** Antigravity (Gemini)  
**Time:** 2026-11-06 ~16:30 IST

> "Refactor and harden the torture oracle in this Go repo. Do not change the engine.
> 1. Move the oracle out of cmd/torture into internal/torture. Keep cmd/torture as main.
> 2. Replace VerifyD2 with signature func VerifyD2(dump map[string][]byte, key string, lastAcked []byte, inFlight [][]byte) error.
> 3. Reject duplicate op-IDs across entire run.
> 4. Enforce limits in Dispatch (one in-flight op per lane, max W total, max 10k batch, op-IDs without colons).
> 5. Make Verify scale: check untouched keys once against committed, enumerate 2^m subsets only over touched keys.
> 6. Make ParseDump return an error on malformed escape sequences."

---

**Tool:** Antigravity (Gemini)  
**Time:** 2026-11-06 ~16:40 IST

> "You are completing the torture tool for the KIPI project in this repo. Work ONLY in cmd/torture/ and tests for the torture tool. Do NOT modify src/kvdb/, cmd/kvdb/, or the engine in any way. Treat the engine as a black box that is invoked through its CLI. Target layout: cmd/torture/ {main.go, session.go, oracle.go, oracle_test.go, gen.go, scale.go}."

---

**Tool:** Antigravity (Gemini)  
**Time:** 2026-11-06 ~16:50 IST

> "implement gen.go with the seeded per-lane sequences and the mode profiles, and add tests for determinism (same seed gives identical sequences) and op-ID uniqueness. Do not implement the process-running parts against a real engine yet. Paste raw output for every command."

---

**Tool:** Antigravity (Gemini)  
**Time:** 2026-11-06 ~17:00 IST

> "go build -o . ./cmd/kvdb ./cmd/torture
> ./torture --iterations 20 --seed 1 --mode default d othis"

---

**Tool:** Antigravity (Gemini)  
**Time:** 2026-11-07 ~01:20 IST

> "i haVE MERGED my branch with backchodi branch , i want u to integrate and make it working"

*(Integrated storage engine from teammate branch, flattened layout to repository root, implemented wire-protocol hex dialect HPUT/HDEL/HBATCH/HGET, and resolved Scenario D2 concurrent key tracking.)*

---

**Tool:** Antigravity (Gemini)  
**Time:** 2026-11-07 ~04:10 IST

> "bro see this and run everything"

*(Ran full verification suite: unit tests, race detector, all 7 torture modes with zero failures, 1M-key scale benchmark, and verified document deliverables.)*

---

## Notes & Engineering Ownership

The AI was used as an assistive tool for:
1. Parsing and synthesizing competition specifications (`Kill It and Prove It-1.pdf` and `nvm.pdf`).
2. Scaffolding Go standard-library implementations according to design parameters.
3. Drafting verification test cases, torture harness permutations, and documentation.

Key design and correctness decisions directed by the human team:
- **Append-only log + in-memory index**: strictly single authoritative data file `db.kv` with advisory OS locks (`flock` on POSIX, `LockFileEx` on Windows).
- **Two fsync barriers**: Barrier 1 (data records) followed by Barrier 2 (commit marker with exact record count) ensures clean torn tail detection without half-committed batches.
- **Oracle independence**: The torture tool oracle never inspects engine internal state; it reasons solely about emitted ACK/ERR promises and asserts recovery against allowable $2^m$ candidate subsets.
- **Key-disjoint dispatch & D2 resolution**: Deterministic per-lane queues with backpressure on key intersection, combined with dedicated D2 state verification for concurrent same-key races.
- **Linear forward recovery scan**: Reads 8 MB buffered blocks, validating headers, length guards, and Castagnoli CRC32C before byte indexing, completing 1,000,000 key recovery in < 500ms.
