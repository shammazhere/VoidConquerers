# PROMPTS.md: AI Prompt Log

Every prompt our team sent to an AI tool during the event, in chronological order, grouped by tool and team member. Prompts are copied exactly as typed, typos included. Where a prompt carried attachments, they are listed in brackets.

---

## 1. Antigravity (CLI and torture tool owners)

**Tool:** Antigravity (Claude Sonnet)
**Time:** 2026-11-06 ~15:30 IST

> "read the nvm pdf and start building cli for this project use commands strictly given in kill it and prove it-1 pdf and build the solution"

**Tool:** Antigravity (Claude Sonnet)
**Time:** 2026-11-06 ~15:42 IST

> "continue"

(The agent resumed after a server restart and generated the CLI, the torture tool, tests and first drafts of the documents.)

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
**Time:** 2026-11-06 ~20:36 IST

> "now i have merged my branch intall with shamaz , i want u to integrate it and make sure everything is working and finalized"

---

**Tool:** Antigravity (Gemini)
**Time:** 2026-11-07 ~01:20 IST

> "i haVE MERGED my branch with backchodi branch , i want u to integrate and make it working"

---

**Tool:** Antigravity (Gemini)
**Time:** 2026-11-07 ~04:10 IST

> "bro see this and run everything"

---

**Tool:** Antigravity (Gemini)
**Time:** 2026-11-07 ~06:45 IST

> "bro deleted everything in the main and merhe the intall branch to the main and comit it has final product and chek whether everyhing is woring properlu"

---

## 2. Claude (claude.ai, engine owner Mohammed)

This session produced the engine (`src/kvdb`), the architecture and design documents, and, at the end, the integration fix patch for the CLI, torture tool and docs described below.

| # | Time (IST) | Prompt |
|:--|:--|:--|
| 1 | *(time: see chat)* | "hello" |
| 2 | *(time: see chat)* | "so this is the things i got from claude and all jst check it and analyze it and decide the best approach and the best thing to do to outshine everyone here" [attachments: earlier design notes] |
| 3 | *(time: see chat)* | "can u ay in brif like in one small paragaraph whats the concept they expect us" |
| 4 | *(time: see chat)* | [Long "STEP 0 / STEP 1" prompt: unzip the "almighty" zip, read the PDF, flag any pre-event code, "Stop and report before continuing if you find a problem"; then build the engine foundation with the frame/FINAL design, Go standard library only, "I must be able to explain every line orally to a judge". **TODO-TEAM: paste the full text verbatim from the chat.**] |
| 5 | *(time: see chat)* | "use this archtecture and create a zip file of project analyse and build all the code and give back a zip file for he whole project" [attachment: v3.2 architecture] |
| 6 | *(time: see chat)* | "give clean structure of the zip file  in a pdf" |
| 7 | *(time: see chat)* | "so did the planning part completed ? … we create a database first , next we create a torture tool and then set database according to all their constraints and then test" |
| 8 | *(time: see chat)* | "soo wth was that .zip file it didnt have anything only 2 files in engine and its not proper idk y ...my antigravity did better ig full functinal" |
| 9 | *(time: see chat)* | "nahh i can see torture file only has 5 files which has .go extensions" |
| 10 | *(time: see chat)* | "jst check this architecture before anything i want u to plan thuroughly without giving out aything cuz 30 teams has same statement so we need to stand out and plan the best one with best architecture and other system designs" [attachment: nvm.pdf] |
| 11 | *(time: see chat)* | "so how many parts are required , i want to divide into 3 parts , one torture part , 2nd is cli and 3rd is engine is it good to go ? and what every part does" |
| 12 | *(time: see chat)* | "so im doing olnly the engine part ok ....i want u to completely give me the zip folder of engine part with complete working and all the constraints it asked for if u dont have context about the requirement pdf ask me ill send it to u" |
| 13 | *(time: see chat)* | "so this is complete ? engine part ill unzip i and start doing other parts ?" |
| 14 | *(time: see chat)* | "cant u jst give me the engine part in seperate zip folder only the part which i asked , other oarts like cli and torture is done by other people so i dont touch it" |
| 15 | *(time: see chat)* | "whats in go.mod" |
| 16 | *(time: see chat)* | "and how to test jst the engine part ?" |
| 17 | *(time: see chat)* | [terminal output pasted] "mohammed@mohammed-Latitude-5400:~/Documents/Hackathon/srinathon 3.0/kipi-engine$ go version / Command 'go' not found, but can be installed with: sudo apt install golang-go # version 2:1.21~2, or sudo apt install gccgo-go …" |
| 18 | *(time: see chat)* | "so this is wht i got after testing it gave 42 seconds its too late right ? btw whats it checking make me understand in brief and summary that what the engine part exactly do and wht we tested right now" [attachment: test output] |
| 19 | *(time: see chat)* | "so now mentor is coming wht should i explain abt our progress and how to" |
| 20 | *(time: see chat)* | "ok so its completely done right ? so i can move from the engine part ?" |
| 21 | *(time: see chat)* | "i want u to create the workflow structure for our complete project with clean and understandable diagram" |
| 22 | *(time: see chat)* | "and high level and low level architecture workflow of database" |
| 23 | *(time: see chat)* | "flow diagram i want ...how professional people use like er diagram omething" |
| 24 | *(time: see chat)* | "make system architecture for this project more clean and attractible" |
| 25 | *(time: see chat)* | "can u do a kind of block diagram good looking one" |
| 26 | *(time: see chat)* | "bruhh er diagram of low level and high level architecture exactly" |
| 27 | 22:39 | "so this is the complete integrated project so i want u to analyze the entire project and as 30 teams are building the same ps everyone are extraoridnary and some have caude pro and some have astra and most of them are top level international winners so never underestimate them , and analyze everything and say like unique differentiator so that we can outshine every other teams present here, according to my understanding since they gave all the 19 page requirement, either its system design part or something like playing with cashe or somethig we need u to teach me in simple and eadily understanding way and which i can explain to judges" [attachments: srinathon_3.0.zip, problem statement PDF] |
| 28 | 22:58 | "yes fix all the issues and send the me the clean working code and also if possible the best possible differentiator that helps us stand out of all teams either in system design as i believe they are intrested in our approach like how we will solve the problem and what method well use so we need to utilize the opportunity to win" |
| 29 | 23:52 | "okay , ig u checked all the parts not only my engine ... other peoples part too and make it the best one like complertely working and fully functinal with the compresing something Summary of Failed Requirements Space Reclaim (Shrink): FAILED. 52,793,992 bytes remained on disk after deleting 1M keys. Auto-compaction was not triggered by delete batches. Engine team must resolve before final judging." |
| 30 | 01:10 (Nov 7) | [pasted a root-cause analysis from another AI tool: "Root cause found and confirmed. Why shrink failed ..." with a proposed patch (1 MiB floor + half-dead rule). **TODO-TEAM: log the prompt that produced it under that tool's section.**] |
| 31 | 01:21 (Nov 7) | "and next is give me the main things which i need to explain like while sending if it interrupts , or if reads half and then interrupts those kind of things and will it go , how it go , and how are we storing it , bnary or something  , and also update the code to display in cli the time taken , how many files got rejected , how many accepted and reason and why and other factors and race conditions what happens if race conditions occurs , and how our system stores like from cache or secondary memory ,in half interupt will it stay incashe or such kind of ..jst now mentor told me they jst are interested in system architecture and only intrested in how things work ... so i dont want u to goo much deep and confuse me jst make me understand it clearly and thuroghly" |
| 32 | 01:48 (Nov 7) | "can u build a high level and low level architecture diagram using draw.io" |
| 33 | 02:06 (Nov 7) | "use draw .io only  use boxex and kinda handmade" |
| 34 | 04:24 (Nov 7) | [attached terminal output of an old build] "the current one u gave when tested it took quite some time while previous one was very speed and it desnt show the time , how many got interrupted and other summary" |
| 35 | 04:56 (Nov 7) | "soo  i want some changes in this part like while writing the records and after records but before marker , would it be best to instead of rejecting it completely isolate them and keep their log seperately so the user continue something like that ?" |
| 36 | 05:16 (Nov 7) | [attached terminal output] "see this its coparatively slow than previous one and the summary is kind of jumbled             cant understand immediately" |
| 37 | 05:30 (Nov 7) | "does this achieve the goal specified in requirement?" |

### What prompt 28 changed (so every line stays explainable)

* `cmd/kvdb/serve.go`: per-lane ordering, both op-id positions, ACK printed inside the commit callback, error codes by `errors.Is`, `COMPACT` / `DUMP` / `HELP`, `--crash-at`, `--step-delay-us`, `--compact-min-bytes`.
* `cmd/kvdb/commands.go`: `open` reports what recovery repaired; `--trace`, `--crash-at`, `--step-delay-us`.
* `cmd/torture`: fresh database on every run; per-mode workloads that really reach their scenario; coverage report; `--mode crash-points` (new file `crashpoints.go`: 15 cases at the 14 crash points, including the client's ACK and a write after compaction, so all four planted bugs fail it deterministically); `--mode scale` (1M keys, kill mid-batch, shrink check); CORRUPT handled as an engine error.
* `go.mod`: `go 1.21` so it builds with the Go in Ubuntu's apt.
* `src/kvdb/write.go`, `recovery.go`, `db.go` (after the teammate's 1M-delete shrink report): auto-compaction floor 64 MiB → 1 MiB, plus "compact when the database is empty"; new test `TestDeleteEverythingShrinks`; scale mode now checks the shrink without sending `COMPACT`.
* `cmd/kvdb` (prompt 31): `open` and `check` print time taken, records accepted, bytes rejected and the reason; `batch` prints put/del counts and time; `serve` has `STATS` and prints a summary on `EXIT` (new file `stats.go`).
* `diagrams/KIPI-architecture.drawio` (prompts 32–33): high- and low-level diagrams in draw.io sketch style.
* `src/kvdb/rejected.go`, `kvdb rejected` (prompt 35): recovery keeps a copy of the cut, uncommitted tail in `db.kv.rejected-*` for inspection; never applied (we rejected replaying it: it would break atomicity and could apply a retried write twice). New test `TestRejectedTailIsSavedNotApplied`.
* `cmd/torture` and `kvdb dump` (prompt 36): about twice as fast. `dump` escaped every binary byte with a `fmt` call and the parser decoded each with an allocation; now a lookup table (same output format). Operations are generated just before they are sent instead of all up front (`LaneGen`, test `TestLaneGensMatchQueues` proves the same seed gives the same ops). Output is now a table with a header and a plain-words summary (Durability / Atomicity / Recovery).
* Prompt 37 (requirement audit against the PDF): F4 fixed (a final marker cut by 1–4 bytes is rebuilt from its constant bytes, CRC-checked and completed, so cutting 1 byte loses nothing; tests updated); failure report now names the op IDs (expected vs found); BREAKING.md cut to two pages as required (long version kept in `docs/BREAKING-detailed.md`); README states the batch order (B5); ARCHITECTURE.md links the draw.io diagram and its images. The re-test found a race in the torture oracle (`d2-same-key --seed 11`, iteration 38: ACKs from two lanes recorded in wake-up order, not commit order); fixed in `cmd/torture/session.go` by recording ACKs in the stdout reader.
* Docs: README, BREAKING, ARCHITECTURE, ENGINE, DESIGN-EXTENSION rewritten to match the code, with measured numbers.

---

## 3. How we used AI, and what we decided ourselves

AI tools generated most of the code and the first drafts of the documents. Our team chose the design, ran every test, and can explain every line. The decisions:

* **Append-only log + in-memory hash index:** the simplest design that satisfies all invariants; nothing is overwritten in place.
* **Two fsyncs per commit (records → fsync → marker → fsync → ACK):** the marker must never reach the disk before the records it commits. A single fsync passes `kill -9` tests but is not safe under power loss (BREAKING.md §2).
* **Every write is a batch with a counted commit marker:** one rule gives single-op and batch atomicity.
* **Linear recovery scan, bounded by auto-compaction:** simple and correct; 1,000,000 keys recover in 486 ms.
* **Corruption guard:** cut a torn tail, but refuse (`CORRUPT`) when damage is followed by committed data.
* **Two kinds of testing:** random kills to find unknown bugs, plus 14 deterministic crash points to prove the known cases, plus planted bugs to prove the tester can fail.
* **Windows platform layer:** `LockFileEx` instead of `flock`, close-rename-reopen instead of renaming over an open file.
