# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:

```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

---

# Project: HoldIt — Data Warehouse Refactor

A stock-screening tool being rebuilt as an end-to-end data warehouse and analytics project.
Working branch: `refactor/de-da`.

## Where the specs live

| Doc | Read it when |
|---|---|
| `docs/architecture.md` | Before touching any model, table, or DAG. The full To-Be spec, plus the decisions behind it |
| `docs/plan.md` | To know what to build next. Steps are ordered and have exit criteria |
| `docs/as-is.md` | To understand *why* a rule exists. Frozen analysis of the old system's failures |

The **Architecture Decisions** section of `docs/architecture.md` records what was considered and
rejected. Read it before proposing an alternative — the alternative has probably already been
rejected for a reason.

## Section 3 does not apply to this refactor

"Don't refactor things that aren't broken" assumes a healthy codebase. This one is being
deliberately dismantled. `docs/plan.md` Step 1 lists what gets deleted; that list is the
authority, not caution. Outside that list, Section 3 applies normally.

## Invariants

Rules that break the architecture if broken. Violating one is a bug even if the code runs.

- **Python moves data, SQL transforms it.** Ingestion derives nothing: no computed values, no
  defaults, no `abs()`, no dropped fields, no fallbacks. Serializing a DataFrame to JSON is
  allowed; computing a CAGR from it is not.
- **Missing is `NULL`, never `0.0`.** A failed fetch or parse records `http_status` and leaves
  the value null. Defaulting to zero is the single worst bug in the old system.
- **Bronze is immutable.** No row is ever UPDATEd or individually DELETEd. A partition is
  replaced only by re-running its own date.
- **JSON is parsed in Silver staging, nowhere else.** Never before Bronze — a wrong parse at
  ingest is permanent, because no API returns last Friday's snapshot.
- **Layer isolation.** Silver reads Bronze, Gold reads Silver, Tableau reads Gold. Never
  further back, never backward. If a Gold model needs a field, fix Silver.
- **Repo = definition, GCP = materialization.** Every query's source of truth is a `.sql` file
  in git. Nothing is authored in the BigQuery console.
- **Reconstructed history never produces a score.** It may inform charts and analysis.
  `fct_metrics` and `fct_metric_scores` read live snapshots only.
- **`price_date` comes from the payload index, never from `snapshot_date`.** Getting this
  backwards collapses five years of prices onto one day.
- **A grain that is not tested is not a grain.** Every model declares its grain in `schema.yml`
  and has a `unique` test on it.
- **`stg_` means "parses a source".** A model that does not read Bronze is never named `stg_`.
  Sources are `yf` and `dart` in every identifier — never `yfinance`.

## Docs are part of the diff

If the code and the docs disagree, one of them is a bug. Decide which and fix it in the same
change. A plan step that turned out wrong should be corrected in `docs/plan.md`, not silently
worked around.
