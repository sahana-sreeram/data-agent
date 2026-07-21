# data-agent

MVP of a self-healing enterprise data-agent project. It generates a deterministic synthetic
baseline dataset (customers, loans, payments) for a simulated lending company, transforms it into
a trusted portfolio summary, independently validates that summary, diagnoses *why* validation
failed when it does, and — for the incident categories where it's safe to automate — proposes,
applies, and deterministically verifies a repair, all inside strict safety boundaries. A CLI
business Q&A agent (`src/ask.py`) closes the loop: ask a question, and if the underlying data is
currently broken, it self-heals via the same diagnose/repair/verify machinery before answering, or
honestly refuses rather than guess.

## Scope (so far)

- Deterministic synthetic data generation (`src/generate_data.py`).
- A pandas transformation from raw data into an aggregate portfolio summary (`src/transform.py`).
- Minimal metadata describing the data, business rules, lineage, and validation rules (`context/`).
- Deterministic validation that independently recomputes every metric from raw data rather than
  trusting the ETL's own output (`src/validate_portfolio.py`).
- Pipeline orchestration that runs transform -> validate end to end and records the outcome
  (`src/run_pipeline.py`).
- Three controlled scenarios, each proving a different, genuinely distinct incident shape:
  an unrecognized upstream value (`data/scenarios/settled_bug/`), an approved rule change the
  ETL config hasn't caught up with (`data/scenarios/settled_rule_adopted/`), and an ETL that
  silently drops valid loans via an incorrect join (`data/scenarios/incorrect_join/`), diagnosed
  using only general-purpose data-investigation tools.
- A read-only diagnosis agent that investigates *why* validation failed, using tools rather than
  a precomputed answer (`src/diagnosis_agent.py` and friends).
- A constrained repair agent and deterministic self-healing workflow: eligibility gate -> repair
  planning -> policy validation -> isolated-workspace apply -> rerun tests/ETL/validation ->
  promote only on full verification (`src/apply_repair.py`, `src/verify_repair.py`,
  `src/run_self_healing.py`, and friends).
- A business Q&A CLI that closes the loop from the original vision: answer a question from trusted
  data directly when validation passes; when it doesn't, run diagnosis and self-healing first and
  answer from the corrected data; and when an incident can't be safely auto-repaired, refuse to
  fabricate a confident number (`src/ask.py`).

## 1. Generate synthetic data

```
python3 -m src.generate_data \
  --num-customers 100 \
  --seed 42 \
  --output-dir data/raw \
  --as-of-date 2026-07-20
```

All arguments have defaults matching the values above, so `python3 -m src.generate_data` alone
regenerates the checked-in `data/raw/` output byte-for-byte. All dates are computed relative to
`--as-of-date`, never the machine clock, so output only changes when an input argument changes.

Default run produces:

- 100 customers
- ~73 loans (target range: 60-80)
- ~740 payments (target range: 300-800)

## Data model

- **customers**: demographic/risk attributes only — no names, addresses, or other sensitive PII.
- **loans**: one loan per record, `scheduled_payment_amount = principal_amount / term_months`
  (interest_rate is descriptive metadata only, not used in payment math).
- **payments**: one record per due date. `ACTIVE` loans carry a recent window of realized
  payments plus near-term `SCHEDULED` ones; `CLOSED` loans carry a full payment schedule whose
  `PAID` amounts reconcile to `principal_amount` within $0.01; `DEFAULTED` loans carry payment
  history up to the point of default, ending in a `LATE`/`MISSED` payment.

## 2. Transform into a portfolio summary

```
python3 -m src.transform \
  --loans-file data/raw/loans.json \
  --payments-file data/raw/payments.json \
  --output-dir data/processed \
  --as-of-date 2026-07-20
```

Reads `loans.json` and `payments.json` with pandas and aggregates them into a single portfolio-wide
summary (not a per-loan table) written to `data/processed/portfolio_summary.json`:

```json
{
  "as_of_date": "2026-07-20",
  "loan_count": 73,
  "active_loan_count": 38,
  "closed_loan_count": 21,
  "defaulted_loan_count": 14,
  "payment_count": 740,
  "successful_payment_count": 596,
  "total_original_principal": 1581087.62,
  "total_successful_payments": 583565.26,
  "total_outstanding_balance": 997522.36
}
```

The headline calculation — and the answer to "what is today's total outstanding loan balance?" — is:

```
total_outstanding_balance = total_original_principal - total_successful_payments
```

**Which statuses count as "successful" is config, not hardcoded logic** — both `transform.py` and
`validate_portfolio.py` load `context/business_rules.json` at runtime. For the MVP:

> Only `PAID` is treated as a successfully settled principal payment. `LATE` payments have
> `amount_paid > 0` (the money was eventually received), but `LATE` is retained as a behavioral
> status and deliberately excluded from the balance calculation. This is an MVP simplification,
> not a claim that late-paid amounts were never collected — a future iteration can enrich this
> rule once "received" is tracked separately from "on time", and that change belongs in
> `context/business_rules.json`, not in the transformation code.

## 3. Minimal metadata (`context/`)

Static, hand-authored context describing the data and rules, mirroring `src/schemas.py`:

- `context/data_dictionary.json` — field-level docs for customers, loans, payments, and the
  portfolio summary output.
- `context/business_rules.json` — which payment statuses are "successful," which enum values are
  valid, and the outstanding-balance formula. Loaded (not just read) by the ETL and validator.
- `context/lineage.json` — the dataset dependency graph: which script produces which file, and
  what it depends on.
- `context/validation_rules.json` — the 14 deterministic checks `validate_portfolio.py` runs, with
  descriptions and tolerances (`$0.01` for currency, exact match for counts).

## 4. Independent validation

```
python3 -m src.validate_portfolio \
  --loans-file data/raw/loans.json \
  --payments-file data/raw/payments.json \
  --summary-file data/processed/portfolio_summary.json \
  --output-dir data/processed
```

Recomputes every metric directly from raw `loans.json`/`payments.json` using its own logic — it
does not call `transform.py`'s calculation and compare the result to itself, since that would let
a bug in that calculation pass unnoticed. Each of the 14 checks (schema, enum validity, referential
integrity, and reconciliation) is written to `data/processed/validation_results.json` with
`expected`/`actual`/`difference`/`details`, so a failure carries concrete diagnostic evidence, not
just a boolean. Exits non-zero if any check fails.

## 5. Pipeline orchestration

```
python3 -m src.run_pipeline \
  --loans-file data/raw/loans.json \
  --payments-file data/raw/payments.json \
  --output-dir data/processed \
  --as-of-date 2026-07-20
```

Runs transform then validate in-process (no subprocess) and writes
`data/processed/pipeline_run.json`:

```json
{
  "as_of_date": "2026-07-20",
  "etl_status": "SUCCESS",
  "validation_status": "PASS",
  "overall_status": "SUCCESS"
}
```

`overall_status` is `SUCCESS` only if the ETL ran without error **and** every validation check
passed. This is the entrypoint a future repair loop will rerun after fixing an upstream break, to
prove the fix actually restored a passing pipeline.

## 6. Proving validation catches a real break: the `settled_bug` scenario

This is the other half of the roadmap: *"changed data → ETL still succeeds → summary is incorrect
→ validation fails with clear diagnostic evidence."* `data/raw/` stays the permanent clean
baseline — nothing here mutates it. Instead:

```
python3 -m src.simulate_upstream_change \
  --payments-file data/raw/payments.json \
  --output-file data/scenarios/settled_bug/payments.json \
  --seed 99 \
  --fraction 0.2
```

Deterministically relabels a seeded 20% of `PAID` payments to `SETTLED` (`amount_paid` and
`payment_date` are untouched — the money was still received, only the status label changed, as if
an upstream system started using new terminology). Then run the same, unmodified pipeline against
the corrupted file with its own output directory:

```
python3 -m src.run_pipeline \
  --loans-file data/raw/loans.json \
  --payments-file data/scenarios/settled_bug/payments.json \
  --output-dir data/scenarios/settled_bug \
  --as-of-date 2026-07-20
```

Result, using the checked-in default run:

| | Clean (`data/processed/`) | Scenario (`data/scenarios/settled_bug/`) |
|---|---|---|
| `etl_status` | SUCCESS | SUCCESS |
| `total_outstanding_balance` | 997522.36 | 1109623.87 (inflated by exactly the $112,101.51 relabeled) |
| `validation_status` | PASS | **FAIL** |
| `overall_status` | SUCCESS | **FAILURE** |

The ETL doesn't crash — it just silently stops counting the relabeled payments as successful,
because `context/business_rules.json` only lists `PAID`. The single failing check is
`payment_status_enum_valid`, which names the exact problem:

```json
{
  "id": "payment_status_enum_valid",
  "status": "FAIL",
  "expected": ["FAILED", "LATE", "MISSED", "PAID", "SCHEDULED"],
  "actual": ["FAILED", "LATE", "MISSED", "PAID", "SCHEDULED", "SETTLED"],
  "details": "unexpected values found: ['SETTLED']"
}
```

**The reconciliation checks (including `total_outstanding_balance_reconciliation`) still PASS** —
the ETL and the validator's independent recomputation agree with each other, because both apply
the identical `PAID`-only filter to the identical corrupted input. This is the key lesson of this
milestone: reconciliation alone can't catch a bug where two independent calculations make the
*same* wrong assumption. It takes a separate domain/enum check — validating that the raw data
itself only contains statuses the system knows about — to catch this class of upstream drift.
No repair logic exists yet; this milestone only proves detection.

## 7. Diagnosis agent: why validation failed, not just that it failed

### Deterministic validation vs. agent diagnosis — two different jobs

`validate_portfolio.py` proves a discrepancy exists. It is ordinary, repeatable Python: same
inputs, same output, every time. It answers "what failed, and by how much." It does **not** answer
"why," because that requires investigation — comparing business intent against source data against
ETL implementation — which isn't reducible to a fixed formula the way `expected - actual` is.

`diagnose_incident.py` and friends are the other half: a genuinely investigative agent. It receives
only the validation failure as its starting signal — never a precomputed root cause — and decides
for itself which read-only tools to call, in what order, to build an evidence-backed hypothesis.
Nothing in this codebase hardcodes "the answer is SETTLED vs PAID" anywhere the model can just
read it off; the model has to call tools and connect what they return.

### Why the agent starts from validation output, not raw data

Handing the agent `validation_results.json`'s failed checks (id, description, expected, actual,
difference, details) — and nothing else — means it starts exactly where a human on-call engineer
would: "this number is wrong, here's by how much." It has to go find out why itself, the same way
a person would open the data and the code.

### Why the tools are read-only

The agent's only job this milestone is to explain a discrepancy, not to fix one. Every tool in
`src/diagnostic_tools.py` returns a fact — a count, a sample, a config fragment, a bounded function
source — never a verdict, a write, or a way to run anything:

- All data is loaded once, at startup, from fixed paths chosen by the CLI (never by the model).
- Tool arguments are validated against what's actually observed (e.g. `get_payment_samples_by_status`
  only accepts a status that really appears in the data, and caps `limit` at 20).
- `get_relevant_etl_source` returns `inspect.getsource()` of one specific function — never an
  arbitrary file path the model could ask for.
- No tool writes a file, calls a subprocess, or reruns the ETL/validator.

### How the agent investigates instead of receiving the answer

`run_diagnosis()` (`src/diagnosis_agent.py`) is a plain tool-calling loop: send the failed checks
plus a system prompt and every tool's schema to the model; dispatch whatever tools it requests;
feed the real results back; repeat (capped at `max_turns`, default 8) until the model calls a
special `submit_diagnosis` tool with its full structured conclusion. The loop does not know or care
which tools get called, in what order, or how many times — it's the same code path whether the
model calls zero tools or all twenty-five.

### General-purpose vs. scenario-specific tools

Most of `src/diagnostic_tools.py`'s surface is scenario-specific (`get_payment_status_counts`,
`get_duplicate_payment_id_counts`, etc.) — useful, but each one only exists because a particular
incident shape needed it. Alongside these, 7 tools are deliberately schema-generic, operating over
a small alias→dataset registry (e.g. `"loans"`, `"payments"`) rather than any specific column
layout, so a future incident shape doesn't necessarily need a bespoke tool at all:

- `list_datasets` / `get_dataset_schema` / `profile_dataset` — what data exists, its columns, and
  basic per-column null/distinct stats.
- `analyze_key_cardinality(dataset, key_columns)` — how many rows share each key value (generalizes
  the event-duplication tool to any dataset/key).
- `compare_dataset_keys(left_dataset, right_dataset, join_keys)` — the set difference between two
  datasets' keys: what's only on the left, only on the right, or matching. This is the tool that
  lets an agent discover "these loan_ids have zero payment rows" without anyone building a
  join-specific tool for it — see the `incorrect_join` scenario (section 9) for exactly this.
- `aggregate_dataset(dataset, group_by, metrics, filters)` / `sample_dataset(dataset, filters, columns, limit)`
  — generic group-by aggregation and bounded row sampling.

Same safety discipline as every other tool: arguments are validated against the dataset's real
columns (`ToolError` otherwise), no raw filesystem paths or arbitrary expressions ever reach the
model, and every one of them returns only facts — never an interpretation.

### Output grounding and schema validation

The model's `submit_diagnosis` call is **not** trusted just because it type-checks against the API's
schema. `src/diagnosis_models.py` independently re-validates:

- Every enum field against fixed Python enums (`diagnosis_status`, `root_cause_category`,
  evidence `source_type`, `confidence`, fix `scope`).
- Every `evidence.source_reference` must be either the name of a tool actually called *this session*
  or one of a small fixed allowlist of real repository file paths — an invented tool name or file
  path is rejected, not silently accepted.
- `affected_metrics` entries must be real `portfolio_summary` field names.
- `recommended_fix.target_file`, if set, must be in the same file allowlist.
- `DIAGNOSED` requires ≥1 evidence item; `NO_INCIDENT` is only valid when validation actually
  passed; `INSUFFICIENT_EVIDENCE` requires a stated evidence gap.

Any violation raises `DiagnosisValidationError`, and `diagnosis.json` is not written — a malformed
or ungrounded diagnosis is an application failure, never coerced into a fake-valid result.

**`initiating_event` vs. `root_cause`**: the schema keeps these separate on purpose. `initiating_event`
is an external trigger that isn't itself broken and needs no repair — e.g. an approved upstream or
business-rule change. `root_cause` is always the specific, *repairable* thing that must change —
e.g. a downstream component that failed to keep up with that change. An approved contract change is
never, by itself, an acceptable `root_cause` for wrong output; the agent's system prompt explicitly
instructs it to keep the two apart, and `initiating_event` is nullable for incidents with no
separate external trigger (e.g. a plain implementation bug).

**A note on `etl_status`**: pipeline metadata reporting `SUCCESS` means only that the ETL executed
without raising an error — it says nothing about whether the output is correct relative to the
currently approved business rules. The system prompt explicitly warns the agent against treating
`etl_status: SUCCESS` as evidence against an ETL/staleness problem; see the `settled_rule_adopted`
scenario below for a concrete case where execution succeeds every time but the output is wrong.

### Running it

Clean pipeline (no incident):

```
python3 -m src.diagnose_incident \
  --validation-results-file data/processed/validation_results.json \
  --summary-file data/processed/portfolio_summary.json \
  --output-dir data/processed
```

This short-circuits to a fixed `NO_INCIDENT` result and **never constructs a model client** — no
API key needed, no cost, no live call.

Changed pipeline (the `settled_bug` scenario):

```
python3 -m src.diagnose_incident \
  --loans-file data/raw/loans.json \
  --payments-file data/scenarios/settled_bug/payments.json \
  --summary-file data/scenarios/settled_bug/portfolio_summary.json \
  --validation-results-file data/scenarios/settled_bug/validation_results.json \
  --pipeline-run-file data/scenarios/settled_bug/pipeline_run.json \
  --output-dir data/scenarios/settled_bug
```

This runs the full investigation and writes `data/scenarios/settled_bug/diagnosis.json`. It exits
`0` even when an incident is found — a nonzero exit is reserved for application failures (missing
artifacts, model/API failure, malformed model output), not for "found a real problem."

### Model configuration

The agent calls OpenAI's chat completions API with tool calling. Copy `.env.example` to `.env` at
the project root and set your key:

```
OPENAI_API_KEY=sk-...
```

`.env` is gitignored and is never read or displayed by anything other than the `openai` SDK at
request time. Model is configurable via `--model` (default `gpt-5`). `--temperature` defaults to
unset (the parameter is omitted from the API call entirely) because current reasoning models like
`gpt-5` reject any explicit temperature, including `0.0`, with a 400 error — pass `--temperature`
explicitly if you point this at a model that supports tuning it.

### Output location

`data/processed/diagnosis.json` for the clean baseline; `data/scenarios/<name>/diagnosis.json` for
a scenario — same convention as `portfolio_summary.json` and `validation_results.json`.

### Safety limitations

- Every tool is read-only; none can write a file, mutate data, or execute a command.
- No `subprocess` usage anywhere in the diagnosis modules (enforced by a static-analysis test).
- The agent cannot rerun the ETL, the validator, or itself.
- `src/transform.py` and `data/raw/*.json` are verified byte-identical before and after a full
  diagnosis run (`tests/test_diagnose_incident.py`).
- Prompts and tool results are kept small and targeted (bounded samples, one function's source,
  small config fragments) — never the full repository or unrelated files.

### How a future repair step will use this

`diagnosis.json`'s `recommended_fix` (`target_file`, `change_summary`, `scope`) is designed to be
the input to a future coding-agent milestone: a minimal, targeted, human-reviewable suggestion —
not an automated edit. This milestone stops at diagnosis; nothing here modifies code.

## 8. A second scenario: an approved contract change (`settled_rule_adopted`)

`settled_bug` and `settled_rule_adopted` look similar (`SETTLED` shows up in raw payments in both)
but represent two genuinely different incidents, and the agent must tell them apart using evidence,
not a hardcoded distinction:

| | `settled_bug` | `settled_rule_adopted` |
|---|---|---|
| Is `SETTLED` in the approved business rules? | **No** — unrecognized value | **Yes** — `valid_payment_statuses` and `successful_payment_statuses` both include it |
| Which check fails? | `payment_status_enum_valid` | `successful_payment_count`, `total_successful_payments`, `total_outstanding_balance` reconciliation |
| What's the right diagnosis? | Genuine uncertainty — nothing confirms `SETTLED` should count as successful. No confident repair should be recommended. | Confident: the ETL's last output predates an approved rule change and needs to be rerun/reconfigured. |

`settled_bug` is **preserved as-is** as the "unknown contract change" case: no automated repair
should ever be recommended for it, since nothing approved confirms `SETTLED`'s meaning.

`settled_rule_adopted` models the opposite: the business rule *has* evolved and been approved, but
the deployed ETL output hasn't caught up. Since `src/transform.py` and `src/validate_portfolio.py`
are both config-driven (they load `business_rules.json` at runtime rather than hardcoding it), the
only way to make the ETL genuinely stale — without touching its code — is to run it under the *old*
rule and validate the result against the *new* one:

```
# 1. The real, unmodified ETL, run against the OLD (current, checked-in) rule:
python3 -m src.transform \
  --loans-file data/raw/loans.json \
  --payments-file data/scenarios/settled_bug/payments.json \
  --business-rules-file context/business_rules.json \
  --output-dir data/scenarios/settled_rule_adopted

# 2. Validate that same (now stale) output against the NEW, approved rule:
python3 -m src.run_pipeline \
  --loans-file data/raw/loans.json \
  --payments-file data/scenarios/settled_bug/payments.json \
  --output-dir data/scenarios/settled_rule_adopted \
  --business-rules-file context/business_rules.json \
  --validation-business-rules-file data/scenarios/settled_rule_adopted/business_rules.json
```

`--validation-business-rules-file` is a `run_pipeline.py` flag added for exactly this: the ETL
stage uses `--business-rules-file`, but the validation stage checks against a different file when
this is set (defaulting to the same file when omitted, so existing behavior is unchanged). Neither
`data/raw/` nor the main `context/business_rules.json` are touched — the new, adopted rule lives
only in `data/scenarios/settled_rule_adopted/business_rules.json`.

Result:

```json
{
  "id": "total_successful_payments_reconciliation",
  "status": "FAIL",
  "expected": 583565.26,
  "actual": 471463.75,
  "difference": -112101.51
}
```

`payment_status_enum_valid` **passes** here (unlike `settled_bug`) because `SETTLED` is approved.
`etl_status` is `SUCCESS` — the ETL never crashed — yet `overall_status` is `FAILURE`, because
`total_successful_payments` is understated and `total_outstanding_balance` is overstated by exactly
the $112,101.51 in `SETTLED` payments.

Diagnose it the same way as any other scenario:

```
python3 -m src.diagnose_incident \
  --loans-file data/raw/loans.json \
  --payments-file data/scenarios/settled_bug/payments.json \
  --summary-file data/scenarios/settled_rule_adopted/portfolio_summary.json \
  --validation-results-file data/scenarios/settled_rule_adopted/validation_results.json \
  --business-rules-file data/scenarios/settled_rule_adopted/business_rules.json \
  --pipeline-run-file data/scenarios/settled_rule_adopted/pipeline_run.json \
  --output-dir data/scenarios/settled_rule_adopted
```

On a real run, the agent (without any hardcoded hint) inspected `get_failed_checks`,
`get_business_rules`, `get_payment_status_counts`, `get_payment_amount_totals_by_status`,
`get_portfolio_summary`, `get_pipeline_run_metadata`, and `get_relevant_etl_source`, then correctly
produced:

```json
{
  "diagnosis_status": "DIAGNOSED",
  "affected_metrics": ["successful_payment_count", "total_successful_payments", "total_outstanding_balance"],
  "root_cause_category": "BUSINESS_RULE_MISMATCH",
  "initiating_event": "Approved business-rule change: successful_payment_statuses = [\"PAID\", \"SETTLED\"].",
  "root_cause": "The ETL run that produced portfolio_summary used an outdated business_rules configuration (PAID-only)... The output is stale relative to the currently approved rule.",
  "confidence": "HIGH"
}
```

Note it explicitly reasoned that `etl_status: SUCCESS` was "consistent with a stale or misconfigured
rule rather than a runtime error" — exactly the distinction the system prompt requires — rather than
treating successful execution as evidence the output must be correct.

> **Update**: this scenario has since been repaired by the self-healing workflow (section 9 below)
> — its checked-in `portfolio_summary.json`/`validation_results.json`/`pipeline_config.json` now
> reflect the healed, `PASS`ing state, and `repair_verification.json` in its directory records the
> full before/after audit trail. The failure walkthrough above is still exactly how to regenerate
> that broken state on demand; nothing about the incident's generation depends on the repair.

## 9. Repair agent: proposing, applying, and verifying a fix

### Three stages, three different authorities

- **Diagnosis** (already covered above) decides *what* likely caused the incident and *what
  category* of fix is appropriate. It never touches files.
- **Repair** decides *which exact allowed change* addresses that diagnosis — a configuration edit
  or a narrow code patch — and produces a structured plan. It never touches files either: applying
  a plan is done entirely by separate, deterministic code, and only after that code independently
  validates the plan against policy.
- **Verification** is the only authority that may mark a repair `VERIFIED`. It reruns tests, the
  ETL, and validation against the *isolated, patched copy* — never the repair agent's own say-so —
  and only promotes the fix into the real repository if every check passes.

This mirrors the diagnosis/validation split from earlier: an LLM proposes, deterministic code
decides. The repair agent **cannot** mark its own repair verified, alter validation to hide a
problem, touch raw data, or write to any file outside a small fixed allowlist.

### Why a third, separate scenario

`data/scenarios/incorrect_join/` is the **headline code-repair demo**;
`data/scenarios/settled_rule_adopted/` is kept as a **smaller integration test** of the
configuration-repair path. Both are genuine ETL bugs, not a config pointer or an unknown value:

- A small deterministic set of newly originated `ACTIVE` loans exist whose first payment isn't due
  yet — valid loans with zero payment records so far (`src/simulate_incorrect_join.py`).
- The buggy ETL (`compute_portfolio_summary_with_payment_join` in `src/transform.py`) aggregates
  successful payments by `loan_id`, then joins that aggregate onto loans with `how="inner"` — so any
  loan with zero successful payments is silently dropped from the **entire portfolio**, not just
  from the payment total: `loan_count`, `active_loan_count`, `total_original_principal`, and
  `total_outstanding_balance` are all understated, while `payment_count`,
  `successful_payment_count`, and `total_successful_payments` reconcile exactly — a useful signal
  that the problem is missing loans, not miscalculated payments.
- `validate_portfolio_with_join_profile` (`src/validate_portfolio.py`) reuses the **unmodified**
  `validate_portfolio()` for every reconciliation check — it never joins anything, so it catches
  this bug with zero code changes of its own — plus one new informational `WARNING` check,
  `loans_without_payment_records_present`, that surfaces the exact loan_ids and total principal
  directly (never a `FAIL` by itself; see the `settled_rule_adopted`-vs-`settled_bug` reconciliation
  distinction above for why `WARNING` and `FAIL` mean different things here).
- Diagnosed using only the **general-purpose** dataset tools from section 7 —
  `compare_dataset_keys("loans", "payments", ["loan_id"])` directly surfaces "5 loan_ids exist in
  loans but not in payments," with samples — no `incorrect_join`-specific tool exists anywhere in
  `src/diagnostic_tools.py`. This is the whole point of building those 7 tools: the exact investigation
  path (`get_failed_checks` → `get_metric_lineage` → `compare_dataset_keys` →
  `get_relevant_etl_source`) that worked here would work the same way for a filter bug, a schema
  drift, or a bug nobody's thought of yet.

`data/scenarios/payment_events_cardinality/` — the event-stream duplication scenario that used to
be the headline demo — is kept as a working, still-passing example (nothing about it was deleted or
broken), but is no longer one of the 3 core scenarios: diagnosing it required 5 diagnostic tools
built specifically around payment-event duplication, which is exactly the scenario-specific pattern
`incorrect_join` was built to avoid.

### The eligibility gate (deterministic, no LLM)

Before a repair model is ever called, `src/repair_models.evaluate_repair_eligibility` inspects
`diagnosis.json` with plain Python:

```
NO_INCIDENT              -> NO_REPAIR_NEEDED
INSUFFICIENT_EVIDENCE    -> HUMAN_REVIEW_REQUIRED
DIAGNOSED, but:
  root_cause_category not in {BUSINESS_RULE_MISMATCH, ETL_LOGIC, DUPLICATION}  -> HUMAN_REVIEW_REQUIRED
  confidence below threshold (default HIGH)                                    -> HUMAN_REVIEW_REQUIRED
  no recommended_fix / target not in the repair-target allowlist               -> HUMAN_REVIEW_REQUIRED
  missing required fields / bad enum values                                    -> INVALID_DIAGNOSIS
otherwise                                                                       -> ELIGIBLE_FOR_REPAIR
```

`SOURCE_CONTRACT_CHANGE` (the `settled_bug` category) is deliberately **never** eligible: by
definition it means the approved rules haven't caught up with an upstream change yet, so business
semantics are genuinely undetermined — that always needs a human, regardless of confidence.
`BUSINESS_RULE_MISMATCH`/`ETL_LOGIC`/`DUPLICATION` mean an authoritative rule already exists and a
downstream component just hasn't applied it — mechanical, safe to automate. Running this against
the real diagnoses confirms the routing:

| Scenario | `root_cause_category` | Eligibility |
|---|---|---|
| `settled_bug` | `SOURCE_CONTRACT_CHANGE` | `HUMAN_REVIEW_REQUIRED` |
| `settled_rule_adopted` | `BUSINESS_RULE_MISMATCH` | `ELIGIBLE_FOR_REPAIR` |
| `incorrect_join` | `ETL_LOGIC` | `ELIGIBLE_FOR_REPAIR` |

### The repair agent's tools and plan-first workflow

`src/repair_tools.py` mirrors the diagnosis agent's design exactly: `RepairTools` loads everything
once from fixed paths, and every tool returns facts (the diagnosis, failed checks, a business-rules
file **by alias** — never a raw path, the current pipeline configuration, bounded ETL source, the
fixed repair-target registry, the relevant test files, a file's hash by alias). **The model never
receives a write-capable tool.** Its only output is a structured plan, submitted via a
`submit_repair_plan` tool call — mirroring `submit_diagnosis` — validated by
`src/repair_models.parse_repair_plan` before anything is applied:

- exactly one `target_file`, which must be in `context/repair_targets.json`'s registry
- `repair_type` must match that target's registered type (`CONFIGURATION_CHANGE` or `CODE_CHANGE`)
- a `CONFIGURATION_CHANGE` must use `patch.format=STRUCTURED_CONFIG_EDIT` — a small list of
  `{field, value}` operations, each checked against that target's registered editable fields and
  allowed values — never free-form text
- a `CODE_CHANGE` must use `patch.format=UNIFIED_DIFF`, bounded in size
- every `evidence_references` entry must match a real `source_reference` from the diagnosis's own
  evidence — no invented justification
- a fixed `PROHIBITED_TARGET_FILES` set (raw data, `validate_portfolio.py`, `validation_rules.json`,
  diagnosis/validation-result artifacts) is rejected even if a target registry entry hypothetically
  named one — defense in depth beyond the positive allowlist

### The real minimal repair for each scenario

For `settled_rule_adopted`, the "adopted" business rules file **already exists** —
`data/scenarios/settled_rule_adopted/business_rules.json` — so nothing needs new content. The
actual bug is that the ETL was *invoked* pointing at the wrong file. `data/scenarios/settled_rule_adopted/pipeline_config.json`
is the small, new artifact this milestone introduces to make that concrete and repairable:

```json
{"business_rules_file": "context/business_rules.json"}
```

The registered repair is a **one-field structured edit** — `business_rules_file` → the adopted
file's path — never touching `src/transform.py` (which is already correctly config-driven) or the
shared `context/business_rules.json` (which stays `PAID`-only forever, since it's what the clean
baseline depends on). The diagnosis itself, having no visibility into this indirection, naturally
recommends the more general `context/business_rules.json` — the repair agent's own
`get_allowed_repair_targets`/`get_pipeline_configuration` tools are what let it discover the real,
precise, registered target instead of proposing the disallowed one. `context/repair_targets.json`
is intentionally broader-agnostic than the diagnosis's suggestion for exactly this reason.

For `incorrect_join`, the registered repair is a **narrow unified diff** to
`compute_portfolio_summary_with_payment_join` — nothing in this codebase hardcodes what that diff
should contain; the model derives it from `get_relevant_etl_source`, `get_business_rules`, and
`compare_dataset_keys`. A real, unscripted run produced:

```python
portfolio = loans_df.merge(
    payments_by_loan.rename("total_paid"), on="loan_id", how="left"
)
# Preserve loans with no successful payments; treat missing totals as zero.
if "total_paid" in portfolio.columns:
    portfolio["total_paid"] = portfolio["total_paid"].fillna(0.0)
else:
    portfolio["total_paid"] = 0.0
```
— independently arriving at "switch the inner join to a left join and treat a missing payment total
as zero," which is exactly the fix the diagnosis called for: preserve every loan, never drop one for
lacking a payment row.

`payment_events_cardinality` (kept as a working, non-headline example) was repaired the same way
against `compute_portfolio_summary_from_payment_events`, deriving a dedup-by-`payment_id` fix from
`get_relevant_etl_source`, `get_business_rules`, and `get_duplicate_payment_id_counts`.

### Isolated workspace, policy validation, and the unified-diff applier

`src/apply_repair.py` creates the workspace with `tempfile.mkdtemp()` and copies in **only the
target file**, mirroring its repo-relative path — nothing else exists there. The patch is applied
to that copy:

- `CONFIGURATION_CHANGE` → `apply_structured_config_edit`, a pure dict update from the
  already-validated operations list.
- `CODE_CHANGE` → `apply_unified_diff`, a small, pure-Python, dependency-free unified-diff applier
  (no `subprocess`, no system `patch` binary). It's deliberately **content-anchored**, not
  line-number-dependent: each hunk is located by searching for its context/removed lines' exact
  content in the original file, because real model-generated diffs frequently omit or miscount line
  numbers in their `@@ ... @@` headers (this is exactly what happened on the first live run against
  `payment_events_cardinality` — a bare `@@` with no numbers — and the content-anchored design
  handles it correctly without any prompt engineering workaround). The patched Python is `compile()`-checked
  before being written; a patch producing no actual change is rejected.

Either way, the **real repository file is never touched** during this stage — only the copy. Policy
also independently re-checks the prohibited-file list and that the patch matches the registered
target type, on top of what `parse_repair_plan` already validated.

### Deterministic verification and promotion

`src/verify_repair.py` reruns the ETL and validator against real source data:

- For a `CONFIGURATION_CHANGE`, it calls the real, unmodified `compute_portfolio_summary`, but
  resolves which business-rules file to use by reading the **patched** `pipeline_config.json` from
  the workspace.
- For a `CODE_CHANGE`, it dynamically imports the **patched copy** of `src/transform.py` via
  `importlib.util.spec_from_file_location` — a fresh, isolated module object, never touching
  `sys.modules['src.transform']` — so the real installed module is completely unaffected until
  promotion.
- **Validation always checks against the authoritative/adopted business rules from the scenario's
  manifest, never against whatever the (possibly still-wrong) patched config says to use.** Using
  the patched config's own rule here would reduce validation to "does the ETL agree with itself,"
  which a repair that fails to fix the real problem could still pass — this exact bug was caught
  and fixed while building the test suite (see `tests/test_verify_repair.py`).

It then confirms, deterministically:
- targeted and full relevant test suites both pass (`pytest.main([...])`, in-process)
- the ETL rerun succeeds and validation's `overall_status` is `PASS`
- every previously-failing check now passes; every previously-passing check still passes
- raw data files and protected files (`validate_portfolio.py`, `validation_rules.json`, the
  diagnosis itself) are byte-identical before and after, via sha256 comparison

Only if **every** check passes does it copy the workspace's patched target file and freshly
computed `portfolio_summary.json`/`validation_results.json`/`pipeline_run.json` over the real
scenario directory, then delete the temp workspace. On any failure, the workspace is deleted and
the real repository is left completely untouched — there is nothing to "roll back" because nothing
real was ever written until every check had already passed.

**Both `settled_rule_adopted` and `incorrect_join` have been repaired this way for real** — their
checked-in files now reflect the post-repair, `VALIDATED` state (see `repair_verification.json` in
each scenario directory for the full before/after audit trail, including exact metric deltas: for
`incorrect_join`, `loan_count` 73→78, `active_loan_count` 38→43, `total_original_principal`
1,581,087.62→1,671,091.05, `total_outstanding_balance` 997,522.36→1,087,525.79 — every delta exactly
the 5 new loans' $90,003.43 total principal). Use `python3 -m src.reset_demo` to restore either
scenario to this healed state after re-breaking it for a repeat demo (see the "Replicating the
demo" note below). `payment_events_cardinality` was also repaired for real earlier and is left in
its healed state; it's just not part of the regularly-reset rotation anymore.

### Running it

```
python3 -m src.run_self_healing --scenario-manifest-file data/scenarios/incorrect_join/repair_manifest.json
python3 -m src.run_self_healing --scenario-manifest-file data/scenarios/settled_rule_adopted/repair_manifest.json
python3 -m src.run_self_healing --scenario-manifest-file data/scenarios/settled_bug/repair_manifest.json
```

Each scenario's `repair_manifest.json` is a small, data-driven descriptor (which files to read,
which ETL/validator pair to rerun, which test files are relevant) — adding a future scenario is a
new manifest and a `context/repair_targets.json` entry, not new branching code. `apply_repair.py`
and `verify_repair.py` are also runnable standalone for the two halves of the flow. Being blocked
for human review, or finding no incident, exits `0` — only a genuine application failure (missing
artifacts, model/API failure, malformed model output) or a repair that was applied but failed
verification exits non-zero.

### Replicating the demo

`settled_rule_adopted` and `incorrect_join` are checked in **already healed** (their last live
repair was promoted for real). To watch `ask.py` diagnose and repair one of them again, break it
first, then reset afterward:

```
# Re-break incorrect_join: revert the ETL to the original buggy inner join.
git checkout -- src/transform.py   # (or hand-revert compute_portfolio_summary_with_payment_join to how="inner")
python3 -m src.run_incorrect_join_pipeline   # regenerates the FAIL state

python3 -m src.ask "What is today's total outstanding loan balance?" \
  --scenario-manifest-file data/scenarios/incorrect_join/repair_manifest.json

python3 -m src.reset_demo   # restores src/transform.py and both scenarios to their healed state
```

(This repo isn't a git repository, so there's no literal `git checkout` to run here — hand-revert
the `how="left"` back to `how="inner"` and remove the `.fillna(0.0)` block, or keep your own backup
copy before experimenting, exactly as `src/reset_demo.py --create-snapshot` does.) `reset_demo.py`
restores exactly the files these two demos mutate (`src/transform.py` plus each scenario's
`portfolio_summary.json`/`validation_results.json`/`pipeline_run.json`/`pipeline_config.json`) from
a one-time snapshot (`demo_snapshot/`) taken while both were healthy, and clears the stale
diagnosis/repair/answer artifacts so the next run starts clean. `settled_bug` and the clean baseline
are untouched by it since neither one ever changes. Live-model behavior is somewhat stochastic —
diagnosis occasionally can't name a concrete `target_file` from the evidence available and correctly
blocks for human review instead of healing; retrying is expected to sometimes be needed, exactly
like any other live run in this project.

### Model configuration

The repair agent reuses `src/model_client.py`'s `OpenAIDiagnosisModelClient` directly — no
duplicated credential handling, same `OPENAI_API_KEY` from `.env`. Its model defaults from the
`REPAIR_MODEL` environment variable (falling back to the same default as diagnosis), independent of
`DIAGNOSIS_MODEL`, via `--model` on any of the three CLIs.

### Generalizability

The abstractions here — typed repair categories, an alias-based tool surface, a data-driven target
registry, plan-first policy validation, isolated apply, deterministic rerun-based verification — are
designed to extend to future incident shapes (renamed fields, new approved enum values, wrong
filters, join-key errors, bounded expression bugs, configuration drift) by adding new manifests and
registry entries. Generalizability comes from that structure, not from broad filesystem or shell
access — the agent's tool surface and write capability stay exactly as narrow as they are today no
matter how many scenarios are added.

### Future expansion

This milestone runs entirely locally: no git worktree (the repo isn't a git repository), no GitHub
pull request, no Spark, no MCP server, no S3/MinIO. The isolated-workspace mechanism would extend
naturally to a git worktree once this becomes a real git repo, and the repair plan's structure
(target file, patch, verification steps, risk level) maps directly onto what a future PR-based
workflow would need.

## 10. Business Q&A agent: closing the loop

Every milestone so far ends at a machine-readable artifact — `validation_results.json`,
`diagnosis.json`, `repair_verification.json`. Nothing yet answered the actual business question
this project set out to answer: *"what is today's total outstanding loan balance?"* — including
detecting that the current answer might be wrong and fixing it before answering. `src/ask.py` is
that front door.

### Design: answer directly, self-heal if needed, never fabricate

`answer_question()` (in `src/ask.py`) does exactly four things, driven by one scenario manifest —
the same manifest `run_self_healing.py` already uses, so a new scenario doesn't need new wiring:

1. Load `validation_results.json`. If `overall_status == "PASS"`, skip straight to answering —
   zero diagnosis/repair model calls for a healthy pipeline, same principle as
   `diagnose_incident.py` never calling a model for a clean run.
2. If it's `"FAIL"`, run `diagnose_incident.run_diagnose_incident` and then
   `run_self_healing.run_self_healing` — the *exact* functions the standalone CLIs call, not a
   reimplementation. If verification comes back `VERIFIED`, reload the now-corrected
   `portfolio_summary.json`/`validation_results.json` and proceed to answering.
3. If diagnosis fails outright, or repair is `BLOCKED`/`NOT_VERIFIED`, construct a deterministic
   `UNRELIABLE_DATA` answer via `answer_models.build_unreliable_data_answer()` — no model call, no
   guess, just an honest refusal with the diagnosis's own reasoning as the caveat.
4. Otherwise, build `business_tools.BusinessTools` from the (now-trustworthy) manifest artifacts
   and run the tool-calling business Q&A agent (`business_agent.run_business_qa`), which must call
   `submit_answer` with a structured, grounded `BusinessAnswer`.

### Grounding: the agent cannot report a fabricated number

`business_agent.py` mirrors `diagnosis_agent.py`/`repair_agent.py`'s shape exactly: a system
prompt, three read-only tools (`get_portfolio_summary`, `get_metric_definition`,
`get_business_rules`), and a `submit_answer` tool that ends the loop. The model chooses *which*
metric answers the question and how to phrase it — but `answer_models.parse_business_answer()`
checks, for every cited metric, that `value` is **byte-identical** to
`portfolio_summary.get(metric_name)` and that `source_reference` names a tool actually called this
session. There is no path from "the model wants to say X" to "X gets printed" without X matching
the trusted data exactly — no rounding, no paraphrasing, no estimating.

### Running it

```
python3 -m src.ask "What is today's total outstanding loan balance?" \
  --scenario-manifest-file data/processed/repair_manifest.json
```

`--scenario-manifest-file` also accepts any of the three scenario manifests
(`data/scenarios/settled_bug/repair_manifest.json`, `.../settled_rule_adopted/...`,
`.../incorrect_join/...`). Model selection follows the same per-stage environment
variables as the rest of the system (`DIAGNOSIS_MODEL`, `REPAIR_MODEL`, and a new `ANSWER_MODEL`),
overridable with `--diagnosis-model`/`--repair-model`/`--answer-model`.

### Three live runs, closing the loop end to end

**Clean baseline — answers directly, no diagnosis or repair call:**

```
$ python3 -m src.ask "What is today's total outstanding loan balance?" \
    --scenario-manifest-file data/processed/repair_manifest.json
Answer
  status: ANSWERED
  As of 2026-07-20, the total outstanding loan balance is 997522.36.
  cited metrics:
    total_outstanding_balance = 997522.36
```

**A freshly re-broken `settled_rule_adopted` — diagnoses, self-heals, then answers.** The scenario
was reset to its original broken state (pipeline config pointed at the stale, pre-approval business
rules, per the regeneration steps in section 8) and handed to `ask.py` with no other intervention:

```
$ python3 -m src.ask "What is today's total outstanding loan balance?" \
    --scenario-manifest-file data/scenarios/settled_rule_adopted/repair_manifest.json
Self-healing
  attempted: True
  diagnosis_status: DIAGNOSED
  root_cause_category: BUSINESS_RULE_MISMATCH
  repair_status: APPLIED
  verification_status: VERIFIED

Answer
  status: ANSWERED
  As of 2026-07-20, the total outstanding loan balance is 997522.36.
  cited metrics:
    total_outstanding_balance = 997522.36
```

As in section 9, diagnosis named `context/business_rules.json` as its (plausible but imprecise)
`recommended_fix.target_file` — enough to pass the eligibility gate — and the repair agent, with a
broader toolset, discovered the real target (`pipeline_config.json`) and fixed that instead. Live
model output is stochastic here: the diagnosis agent has no tool exposing the ETL's actual
invocation config, so on some runs it reports `recommended_fix.target_file: null` ("rerun with the
current rules" without naming *where* the stale pointer lives) — which is honest given the
evidence available, but doesn't give the deterministic eligibility gate anything to act on, so that
run correctly ends in `BLOCKED`/`UNRELIABLE_DATA` instead. Both outcomes are the system behaving
correctly for what it could actually establish from evidence.

**`settled_bug` — investigates, and correctly refuses rather than guess:**

```
$ python3 -m src.ask "What is today's total outstanding loan balance?" \
    --scenario-manifest-file data/scenarios/settled_bug/repair_manifest.json
Self-healing
  attempted: True
  diagnosis_status: DIAGNOSED
  root_cause_category: BUSINESS_RULE_MISMATCH
  repair_status: BLOCKED
  verification_status: BLOCKED

Answer
  status: UNRELIABLE_DATA
  I can't give a reliable number right now -- the underlying data failed validation and could not be automatically repaired.
  caveats:
    - Validation failed (BUSINESS_RULE_MISMATCH): Raw payments introduced a new payment_status value 'SETTLED' that is not included in the approved valid_payment_statuses list or the data dictionary. [...] Automated repair was blocked -- human review is required before this number can be trusted.
```

On this run the repair agent got as far as proposing a patch (adding `SETTLED` to
`valid_payment_statuses` only, correctly declining to also add it to
`successful_payment_statuses` without approval) — but the proposed patch produced no actual content
change against the target file, so `apply_repair.py`'s policy check rejected it as a no-op rather
than promoting a fix that doesn't fix anything. Either way — blocked at the eligibility gate or
blocked at patch application — `settled_bug` never produces a confidently-fabricated number, which
is exactly the point of keeping it as the "genuine uncertainty" scenario.

### Safety and scope

`ask.py` never invents a `submit_answer`-shaped dict itself and never decides `VERIFIED`/`PASS` —
those calls stay inside `verify_repair.py`/`validate_portfolio.py`, unchanged. Every failure mode
that isn't a missing input file (a bad model response, a blocked repair, an unverified repair, an
agent that never calls `submit_answer`) degrades to a deterministic `UNRELIABLE_DATA` answer and
exit code `0` — this CLI's job is to answer a business question honestly, including "I can't
reliably answer that right now," not to crash. Only a missing manifest or missing base artifact
(`AskError`) is treated as an application failure.

## Layout

- `src/schemas.py` — enums and record type definitions for customers, loans, payments.
- `src/generate_data.py` — CLI entrypoint and generation logic for synthetic data.
- `src/transform.py` — CLI entrypoint and pandas logic for the portfolio summary.
- `src/validate_portfolio.py` — CLI entrypoint and independent reconciliation logic.
- `src/run_pipeline.py` — orchestrates transform -> validate and records the run outcome; supports
  validating the ETL's output against a different business-rules file than the ETL used, to model
  a rule change the ETL's last run predates.
- `src/simulate_upstream_change.py` — deterministically relabels PAID payments to SETTLED in a
  scenario copy, without touching the clean baseline.
- `src/diagnostic_tools.py` — read-only, allowlisted investigation tools (facts only, no diagnoses).
- `src/diagnosis_models.py` — the structured diagnosis schema and its grounding/enum validation.
- `src/model_client.py` — `DiagnosisModelClient` protocol, the OpenAI implementation, and a scripted
  fake used throughout the test suite.
- `src/diagnosis_agent.py` — the tool-calling reasoning loop and system prompt.
- `src/diagnose_incident.py` — CLI entrypoint tying it all together.
- `src/simulate_payment_events_migration.py` — deterministically expands clean payments into a
  lifecycle event stream with seeded at-least-once `SETTLED` replays (non-headline example, kept
  working; see `incorrect_join` for the current headline code-repair demo).
- `src/run_payment_events_pipeline.py` — the event-stream sibling of `run_pipeline.py`, using
  `compute_portfolio_summary_from_payment_events`/`validate_portfolio_from_payment_events`.
- `src/simulate_incorrect_join.py` — deterministically generates a small set of newly originated,
  valid, no-payment loans in a scenario copy of `loans.json`, without touching the clean baseline.
- `src/run_incorrect_join_pipeline.py` — the incorrect-join sibling of `run_pipeline.py`, using
  `compute_portfolio_summary_with_payment_join`/`validate_portfolio_with_join_profile`.
- `src/reset_demo.py` — restores `src/transform.py` and the `settled_rule_adopted`/`incorrect_join`
  scenario outputs to their known-good healed state from a one-time snapshot (`demo_snapshot/`), for
  repeat live demos.
- `src/repair_models.py` — `evaluate_repair_eligibility` (the deterministic gate) and the structured
  repair-plan schema/grounding validation.
- `src/repair_tools.py` — read-only, alias-based repair-planning tools (mirrors `diagnostic_tools.py`).
- `src/repair_agent.py` — the repair agent's tool-calling planning loop and system prompt.
- `src/apply_repair.py` — eligibility gate, repair planning, policy validation, isolated-workspace
  patch application (includes the content-anchored unified-diff applier).
- `src/verify_repair.py` — deterministic rerun-based verification and promotion.
- `src/run_self_healing.py` — CLI composing `apply_repair` + `verify_repair` end to end.
- `src/business_tools.py` — read-only tools over the trusted portfolio summary for the Q&A agent
  (mirrors `diagnostic_tools.py`/`repair_tools.py`).
- `src/answer_models.py` — the structured business-answer schema, including the exact-value
  grounding check against the trusted portfolio summary, and the deterministic `UNRELIABLE_DATA`
  constructor used when data can't be trusted or repaired.
- `src/business_agent.py` — the business Q&A agent's tool-calling loop and system prompt.
- `src/ask.py` — CLI entrypoint closing the loop: answer directly, self-heal via
  `diagnose_incident`/`run_self_healing` first if validation is currently failing, or refuse to
  fabricate an answer if it can't be verified/repaired.
- `data/processed/repair_manifest.json` — the clean-baseline scenario manifest, so `ask.py` treats
  the healthy pipeline through the same manifest interface as the three incident scenarios.
- `context/` — data dictionary, business rules, lineage, validation rules, and the repair-target
  registry (`repair_targets.json`).
- `data/raw/` — generated customers/loans/payments JSON (the permanent clean baseline).
- `data/processed/` — portfolio summary, validation results, pipeline run record, and diagnosis
  for the clean baseline.
- `data/scenarios/settled_bug/` — unrecognized-value scenario: corrupted payments plus its own
  pipeline outputs, diagnosis, and repair artifacts (blocked, `HUMAN_REVIEW_REQUIRED`).
- `data/scenarios/settled_rule_adopted/` — approved-rule-change / config-repair integration test:
  its own `business_rules.json` (SETTLED approved) and `pipeline_config.json` (the execution
  pointer that was stale, now repaired), plus the full diagnosis/repair/verification audit trail.
- `data/scenarios/incorrect_join/` — the headline code-repair demo: a scenario-local `loans.json`
  with a small set of newly originated, valid, no-payment loans, its own `validation_rules.json`
  (adds one informational WARNING check), and the full diagnosis/repair/verification audit trail for
  the now-fixed `compute_portfolio_summary_with_payment_join`. Diagnosed using only the
  general-purpose dataset tools (section 7) — no scenario-specific tool exists for it.
- `data/scenarios/payment_events_cardinality/` — a working, non-headline example: a lifecycle event
  stream with seeded `SETTLED` replays, its own business rules (`payment_event_rules`), and the
  full diagnosis/repair/verification audit trail for the now-fixed
  `compute_portfolio_summary_from_payment_events`. Superseded by `incorrect_join` as the headline
  demo because diagnosing it needed 5 tools built specifically for payment-event duplication.
- `demo_snapshot/` — a one-time snapshot of `src/transform.py` and the `settled_rule_adopted`/
  `incorrect_join` scenario outputs, taken while both were healthy, for `src/reset_demo.py` to
  restore from after a repeat live demo.
- `tests/` — pytest suite validating referential integrity, enum/range validity, business rules,
  transformation arithmetic, validation failure reporting, upstream-change detection, diagnosis and
  repair grounding/safety, general-purpose dataset-tool behavior, isolated-workspace application,
  deterministic verification, business Q&A grounding/self-healing orchestration, and determinism.
  No test calls a live model.

## Running tests

```
python3 -m pytest
```
