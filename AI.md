# Use of AI in this project

This document covers the assignment's requirement to *describe the development
workflow and say which parts were accelerated by AI versus done by hand*, and to
*provide detailed information on all prompts used and where a prompt fell short
requiring manual intervention*.

- [AI.md at the repo root](#) — this file
- [README.md](README.md) — system overview and build instructions
- [docs/architecture.md](docs/architecture.md) — design decisions and rationale
- [docs/operations.md](docs/operations.md) — the operational runbook

---

## 0. Scope and honesty statement

Read this first, because it affects how much you should trust section 1 versus
sections 2 and 3.

| Marker | Meaning |
|---|---|
| ✅ **Verbatim** | A prompt actually issued during development, quoted as written (including its typos) |
| 🔁 **Reconstructed** | A prompt *reconstructed* from the code, tests and comments it produced. The **decision is real and evidenced by file:line**; the **wording is mine**, written afterwards to describe the question that would have produced it |

Sections 2–5 use 🔁 throughout. The AI session that built this system is long, and
the transcript predating the recorded window is summarised rather than verbatim.
Rather than invent quotes that look authentic, those prompts are marked 🔁 and each
one cites the artefact it produced.

Two things this document will not claim:

1. **That a prompt alone produced a design.** In every case below the prompt
   asked for options and trade-offs, and a human chose. The value was in having
   the alternatives laid out with their failure modes, not in delegating the
   decision.
2. **That the documented outcomes were all correct first time.** Section 5 is the
   honest accounting, and it is longer than a success story would be.

---

## 1. Repo bootstrap

The brief was pasted in full. No code was written until the component breakdown
and the hardest decision per component were stated back.

**Prompt (🔁 reconstructed)**

> Read the brief carefully. Before writing any code, give me the component
> breakdown, the data flow between components, and the hardest design decision in
> each one. Don't start coding yet.

**Prompt (🔁 reconstructed)**

> What are the real design decisions here, and for each one tell me what the
> obvious alternative is and why we're not doing that. I want to defend these
> choices in a review.

**What this produced.** The second prompt is why nearly every non-obvious file in
this repo opens with a *why*. Representative examples, each verifiable:

| Decision | Where the rationale lives |
|---|---|
| Kubernetes Lease, not Raft, for 2-node election | `messageQueue/internal/election/election.go:5-12` — "Raft needs an odd number of voters for quorum, so with two nodes it buys no fault tolerance over the Lease while adding an etcd-class dependency" |
| Plain REST for the Lease, not client-go | `messageQueue/internal/election/kube.go:5-12` — "drags roughly forty transitive modules into a module whose entire dependency set is currently google.golang.org/grpc" |
| `ORDER BY (device_id, source_ts, event_id)` | `collector/schema/schema.go:1-15` — chosen so one key serves both dedup and all three read APIs |
| WAL frames as length-prefixed JSON | `messageQueue/internal/mq/walstore.go:71-73` — "self-describing and forward-tolerant" rather than a binary layout coupled to the struct |

**Result:** 89 Go files / ~20k lines across 4 modules, 6 Helm charts, 9 scripts,
42 Makefile targets, 9 docs — all traceable to a decision record rather than to
guesswork.

---

## 2. Code bootstrap

Grouped by the decision each prompt drove. Every entry is 🔁 reconstructed.

### 2.1 Custom message queue (WAL + durability)

| Prompt | Decision it drove |
|---|---|
| We need durability. Should we use a real broker like Kafka, or build it? | Length-prefixed JSON WAL (`walstore.go:71-73`) |
| Define precisely when it is safe to ack a publisher. Write it as a contract. | fsync-before-ack, per mutation (`walstore.go:100-104`) — "an acked event is on disk" |
| What happens if we crash mid-write to the WAL? | Torn tail truncated, interior corruption fatal (`walstore.go:154-155`) |
| The WAL grows forever and restart replays all of it. Fix that. | Size-triggered rewrite to checkpoint + retained tail (`walstore.go:440-442`) |
| What stops two MQ processes opening the same data dir? | `flock`-exclusive (`walstore.go:124-137`) |
| How do we stop two nodes both accepting writes? | Role as a **local atomic**, not an elector query (`leader.go:49-54`) |
| A partitioned leader can't reach the API server to remove itself. How does it stop receiving writes? | Lease-aware `/readyz`, evaluated by kubelet locally (`main.go:373-387`) |

That last one is the subtlest result: the node that most needs fencing is the one
that can no longer coordinate, so the answer cannot involve the coordinator.

### 2.2 Multi-streamer CSV, no overlap

| Prompt | Decision it drove |
|---|---|
| Multiple streamers read the same CSV. Split rows so no two process the same line and none is skipped — can it be done without coordination? | Pure modulo, `currentLineNum % total == index` (`scheduler.go:5-17`) |
| Deployment pod names are random, so I can't get my index from my name. Where does it come from? | MQ partition registry, and **only** that (`streamer.go:65-67`) |
| If I scale up, all pods boot at once and each sees a partial replica set. What goes wrong? | Startup stability gate — publish only after two identical polls (`configwatch.go:17-23`) |
| On SIGTERM how do we not lose buffered events? | Drain → flush → **wait for acks** → bounded (`batcher.go:80-84`): "gRPC Send() only buffers locally, so cancelling before acks arrive would drop already-'sent' events" |
| Should we leave the partition registry at the start or end of shutdown? | **After** the drain (`streamer.go:134-137`) |

Overlap-freedom is not asserted in a comment; it is a test that fails if broken:
`TestNoDuplicatesAcrossReplicas` — "line %d owned by %d replicas, want exactly 1
(no dupes, no gaps)".

### 2.3 ClickHouse

| Prompt | Decision it drove |
|---|---|
| We are at-least-once, so the same event will be written more than once. ClickHouse has no unique constraint. Where does dedup go? | `ReplacingMergeTree` at the storage layer (`sink.go:1-13`) |
| The ORDER BY key has to do two jobs — collapse duplicates AND make the API fast. Pick the column order and justify it. | `(device_id, source_ts, event_id)` (`schema.go:1-15`) |
| ReplacingMergeTree needs a version column. Which one? | `received_at`, not `source_ts` — which is identical across redeliveries and so cannot order versions |
| Single ClickHouse is a single point of failure. What do we need? | 2 replicas + 3 Keeper (`keeper-statefulset.yaml:1-6`: "Do not run it with an even count") |
| Should we set `insert_quorum=2` so an ack means both replicas have it? | **Rejected** (`operations.md`): "writes fail whenever one of two replicas is down, which defeats HA" |

### 2.4 Idempotency end-to-end

| Prompt | Decision it drove |
|---|---|
| When should the collector commit its MQ offset — before or after writing to ClickHouse? | **After** (`collector.go:1-6`) — the single most important ordering in the pipeline |
| If ClickHouse is hard down, should the collector block? I think wedging is worse than losing. Argue it. | Fail-open + dead-letter, with the boundary stated (`collector.go:256-266`) |
| Paging by OFFSET would be simpler. What's wrong with it on a table being written continuously? | Resume-key paging on `(source_ts, event_id)` (`store.go`) |

---

## 3. Unit-test development

| Prompt | Effect |
|---|---|
| Write tests that pin the *reasons*, not just the happy path. For each design decision, write the test that fails if someone removes the reason. | Test names became failure modes: `TestRewindIsDistinctFromCommit`, `TestLeaderReseedsFollowerResumePastTail`, `TestPromotionRebuildsDedupWindow`, `TestWalTornTailIsIgnored`, `TestNoDuplicatesAcrossReplicas`, `TestReanchorSettlesInsteadOfSpinning` |
| Run the existing unit tests first | Prevented re-running a destructive failover test before confirming the premise |
| Add a test that proves sharding is duplicate-free AND gap-free across any replica count, not just the ones we tried | Property-style assertion rather than three fixed cases |

**Result — 249 test functions across 4 modules:**

| Module | Tests | Statement coverage |
|---|---:|---:|
| streamer | 37 | 52.1% |
| collector | 38 | 47.2% |
| messageQueue | 94 | 68.7% |
| apiGateway | 80 | 57.4% |
| **total** | **249** | **60.3%** (1,823/3,024) |

Reproduce with `make coverage`; `make coverage-html` writes browsable reports.

The brief's unit-test requirement is met by these. The **system** tests are the
bonus: `scripts/verify-deployment.sh` runs 43–47 assertions against a live cluster,
including killing the MQ leader and asserting offsets stay contiguous. The last
full run passed 46.

### Known test-coverage gap

Both `internal/mqclient` packages have **0% statement coverage**. The race
detector passes across all four modules, but it can only prove race-freedom on
paths tests actually execute — and the streamer's resend-on-reconnect logic
(`sendUntil`/`readAcks`), which is where a concurrency bug would most plausibly
live, is never executed by a test. This is documented as gap G8 in
[docs/failure-modes.md](docs/failure-modes.md).

---

## 4. Build environment bootstrap

| Prompt | Effect |
|---|---|
| Build all services with one command and a tag that changes every time, so a stale image can never deploy by accident | `scripts/build-images.sh`, `IMG_TAG=…`, `kind load` |
| Write one script that goes from an empty machine to a verified working system, with assertions at each step | `scripts/quickstart.sh` — cluster, build, load, deploy, verify |
| Don't put credentials in Helm values — `helm get values` prints them in plaintext | Externally-created Secrets, `secretKeyRef`, password hashes only |
| Default-deny all, then open only what each component needs. Document every rule. | `networkpolicy.yaml` with per-rule rationale comments |

Then, from the assignment's own requirements:

| Prompt | Effect |
|---|---|
| The brief requires coverage and OpenAPI generation through a Makefile | 42-target `Makefile`; `make coverage`, `make openapi`, `make check` |
| can we clean up every cluster and tell me the script | `scripts/kind-up.sh --down`, `make cleanup` / `prune` / `prune-all` |
| If the disk is full the script won't work — add a preflight check | `scripts/lib/preflight.sh`, run by both `quickstart.sh` and `kind-up.sh` |

---

## 5. Where prompts fell short, and the manual work that followed

The brief asks specifically for this, so it is the longest section.

### 5.1 A bug report that described a different codebase

**Prompt (✅ verbatim)**

> Fix consumer reconnect to re-send SUBSCRIBE
> What: After readLoop reconnects at client.go: 134-136, re-send the SUBSCRIBE frame
> Why: Any broker restart permanently silences all collectors
> Risk if not fixed: Complete collector blackout — unacceptable in production

**Perfectly formed. Entirely inapplicable.** Three checks: `SUBSCRIBE` appears
**zero** times in the repo (no `.go`, no `.proto`); no `readLoop` function exists;
and `collector/internal/mqclient/client.go` is 179 lines in which the reconnect
area (line 134) sits inside `JoinPartition`, a plain unary RPC.

**Manual work:** grepped for the symbol, confirmed absence, then verified the
*underlying* risk empirically instead of arguing — killed the MQ leader on the live
cluster. The survivor was promoted in under 10 seconds, the collector never dropped
Ready, and the offset audit showed 0 missing and 0 duplicate offsets.

**Lesson retained:** verify the file and line in the report exist before acting on
the report. The template is good; the premise still has to be checked.

### 5.2 An affirmative answer that had to be converted into a proof

**Prompt (✅ verbatim)**

> so there is no data loss

"Looks fine" would have been worthless. The check was escalated into a
set-equality proof over `mq_offset` against the published range, which is what
established `rows == distinct offsets, min 0, missing 0`.

**That proof then found something real:** 34 event_ids had been written more than
once. Not data loss — but it proved at-least-once duplicate delivery was live in
the system rather than theoretical, and it exposed that `README.md` claimed
**"Exactly-once ingestion"**, which is false.

### 5.3 A browser failure that contradicted a passing test

**Prompt (✅ verbatim)**

> i opend this, http://localhost:30911/metrics, it tells page not working

`curl` had returned metrics successfully the whole time. The report was correct
and my verification was insufficient. Root cause: **Colima's port forwarder keeps
accepting connections for a container that no longer exists** and answers every one
with an empty reply, and it does not recover on its own. The fix was to move the
bridge onto `kubectl port-forward`, which sidesteps the VM's network layer.

**Manual work:** traced it to a stale listener rather than accepting either the
user's report or my own test.

**Lesson retained:** report the failure even when your own check passes — especially
then.

### 5.4 Documentation that described removed designs

An audit compared every doc claim against the code. **22 factual errors** were
found and fixed, including two that would mislead an operator badly:

- `docs/architecture.md` stated *"No auto leader election: promotion/demotion is a
  manual, scripted operation."* Election was implemented, chart-enabled, RBAC'd and
  test-covered.
- `docs/components/apiGateway.md` stated *"No auth on the API"* while every
  `/api/v1` route sits behind bearer auth, and omitted `POST /api/v1/token`
  entirely.
- `docs/components/streamer.md` had the **collector's** `/readyz` payload pasted in,
  including the `port-forward deploy/telemetry-collector` command.

A parallel audit of per-component docs found **11 undocumented environment
variables** and **11 of the MQ's 16 undocumented CLI flags**.

**Manual work:** all cross-checking, and every judgement about severity. Neither
audit was asked for; both found defects that reading the prose alone would not.

### 5.5 Correcting the AI when it was confidently wrong

| The claim | Reality |
|---|---|
| "Collecting extra replicas adds HA" | They do **not** shard. `CONSUMER_ID` is a fixed value, so the registry hands all 10 replicas index 0 of 1 and each writes the whole log. Observed live: 34 duplicated event_ids. Logged as G10b |
| Depth is "unacked by everyone" | It is unacked by **at least one** active consumer — the trim barrier is the minimum committed offset across the group |
| Sync-replication acks "stall until the follower reconnects" | Bounded at 5s, after which the leader **fails the publish RPC** and the producer resends |
| `event_id` determines the version | The grouping key is `(device_id, source_ts, event_id)`; `received_at` is the version column |
| The follower's queue depth means it is a separate queue | It is a replica; the leader Service routes all clients to the leader only |

Each was corrected after being challenged with a falsifiable question. None were
accepted on the first answer.

### 5.6 Smaller recurring failures

- Test code repeatedly called real APIs with the wrong signatures
  (`CommitOffset`, `JoinPartition`, a `promoteLocked` that does not exist) and had
  to be corrected over several rounds before compiling.
- Three metrics bugs were only found by writing the test that would catch them: a
  depth accessor that returned 0 for any non-durable store, an inverted meaning for
  `base`, and an `atomic.Value` that panics when handed nil.
- Two documentation edits I made **mangled Markdown tables** and one invented a
  non-existent env var (`API_TOKEN_SIGNING_KEY_FILE`). Both were caught by
  re-reading the rendered output rather than assuming the edit was clean.

### 5.7 Problems AI did not solve

- **The `internal/mqclient` 0% coverage gap** (§3) was identified, not fixed.
- **The lease-flapping incident** — 39 leadership transitions and 1.43M unacked
  events — was diagnosed far enough to name the mechanism (a follower not connected
  so `-sync-replicas 1` can never be satisfied) and no further. It needs a decision
  about repair strategy that has not been made.
- **The shared `CONSUMER_ID` defect** is documented, not fixed, because the fix
  changes partitioning semantics and deserves a deliberate choice.

---

## 6. What was AI-assisted versus manual — summary

| Area | AI did | Human did |
|---|---|---|
| Design | Enumerated alternatives and failure modes for each decision; wrote the rationale into doc comments | Chose every option; rejected Raft, client-go, `insert_quorum`, band-based sharding, OFFSET paging |
| Implementation | Wrote most Go, Helm and script code from the chosen design | Reviewed and corrected throughout; several correctness bugs came from here |
| Tests | Wrote 249 tests, deliberately named after the failure they prevent | Chose the "pin the reason, not the path" strategy and which invariants matter |
| Build/deploy | Wrote 9 scripts, 6 charts, 42 Make targets; added disk preflight after being asked | Decided the tag-every-build rule and the credential-handling policy |
| Documentation | Wrote 9 docs; then audited them against code and fixed 22 factual errors | Set the accuracy standard that prompted the audit |
| Verification | Ran tests, race detector, coverage, live failover tests | Decided what "verified" had to mean — e.g. demanding a proof rather than an assurance for "is there data loss" |

**Net:** the acceleration was in breadth and in having trade-offs enumerated with
their failure modes stated. It was not faster because the work was smaller — the
correctness work in section 5 was slower, and that is where the value was.

---

## 7. Reproducing the verification claims

```bash
make check        # gofmt, go vet, go mod tidy, helm lint, openapi staleness, unit tests with -race
make coverage     # statement coverage per module and combined
make preflight    # tooling and free disk space on the Docker volume
make quickstart   # build, deploy and verify a live cluster end to end
```

---

## 8. Compliance with the brief's requirements

| Requirement | Status |
|---|---|
| How was the repo bootstrapped? Prompts documented | §1 |
| How was the code bootstrapped? Prompts documented | §2 |
| How were the unit tests developed? Prompts documented | §3 |
| How was the build environment bootstrapped? Prompts documented | §4 |
| Detailed information on all prompts used | §1–§4, with ✅/🔁 provenance on every entry |
| Where a prompt fell short and manual work was required | §5 |
| Which parts were AI-accelerated vs manual | §6 |
| README description of AI assistance | [README.md](README.md) → *AI assistance* |