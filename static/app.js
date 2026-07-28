// Console frontend for src/api.py. Vanilla JS, no build step, no framework -- deliberately
// simple. All model/user-provided text is set via textContent (never innerHTML) to avoid
// rendering anything as HTML.

function el(tag, opts = {}) {
  const node = document.createElement(tag);
  if (opts.className) node.className = opts.className;
  if (opts.text !== undefined) node.textContent = opts.text;
  if (opts.attrs) for (const [k, v] of Object.entries(opts.attrs)) node.setAttribute(k, v);
  for (const child of opts.children || []) node.appendChild(child);
  return node;
}

function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function fmtNumber(n) {
  return typeof n === "number" ? n.toLocaleString() : String(n);
}

function fmtBytes(n) {
  if (typeof n !== "number") return String(n);
  return `${(n / 1e6).toFixed(1)} MB`;
}

// --- Tab navigation ---------------------------------------------------------------------

const TAB_LOADERS = {
  overview: loadOverview,
  "data-products": loadDataProducts,
  context: loadContextTab,
  evaluations: loadEvaluations,
};

function switchTab(tabName) {
  for (const button of document.querySelectorAll(".tab-button")) {
    button.classList.toggle("active", button.dataset.tab === tabName);
  }
  for (const panel of document.querySelectorAll(".tab-panel")) {
    panel.classList.toggle("hidden", panel.id !== `tab-${tabName}`);
  }
  const loader = TAB_LOADERS[tabName];
  if (loader) loader();
}

for (const button of document.querySelectorAll(".tab-button")) {
  button.addEventListener("click", () => switchTab(button.dataset.tab));
}

// --- Health banner ------------------------------------------------------------------------

async function loadHealth() {
  const banner = document.getElementById("health-banner");
  try {
    const res = await fetch("/api/health");
    const data = await res.json();
    if (!res.ok) {
      banner.textContent = `Health check failed: ${data.detail || res.statusText}`;
      banner.className = "banner bad";
      return;
    }
    const pipelines = data.pipelines || {};
    const names = Object.keys(pipelines);
    const failing = names.filter((n) => pipelines[n].etl_status !== "SUCCESS" || pipelines[n].validation_status !== "PASS");
    if (failing.length === 0) {
      banner.textContent = `All ${names.length} pipelines healthy`;
      banner.className = "banner ok";
    } else {
      banner.textContent = `${failing.length} of ${names.length} pipelines failing: ${failing.join(", ")}`;
      banner.className = "banner bad";
    }
  } catch (err) {
    banner.textContent = "Could not reach the API.";
    banner.className = "banner bad";
  }
}

// --- Shared renderers (answer / diagnosis / repair / verification stages) ---------------

function renderAnswer(container, answer) {
  clear(container);
  const badge = el("span", { className: `status-badge ${answer.answer_status}`, text: answer.answer_status });
  container.appendChild(badge);
  container.appendChild(el("p", { text: answer.answer_summary }));

  if (answer.cited_metrics && answer.cited_metrics.length) {
    const table = el("table");
    const thead = el("tr", {
      children: [el("th", { text: "Metric" }), el("th", { text: "Value" }), el("th", { text: "Source" })],
    });
    table.appendChild(el("thead", { children: [thead] }));
    const tbody = el("tbody");
    for (const m of answer.cited_metrics) {
      tbody.appendChild(
        el("tr", {
          children: [
            el("td", { text: m.metric_name }),
            el("td", { text: String(m.value) }),
            el("td", { text: m.source_reference }),
          ],
        })
      );
    }
    table.appendChild(tbody);
    container.appendChild(table);
  }

  if (answer.caveats && answer.caveats.length) {
    const ul = el("ul", { className: "caveats" });
    for (const c of answer.caveats) ul.appendChild(el("li", { text: c }));
    container.appendChild(ul);
  }
}

function stageStatus(text, good) {
  return el("span", { className: `stage-status ${good ? "good" : "bad"}`, text });
}

function renderDiagnosisStage(diagnosis) {
  const stage = el("div", { className: "stage" });
  stage.appendChild(el("div", { className: "stage-label", text: "Diagnosis" }));
  if (!diagnosis) {
    stage.appendChild(el("p", { text: "(no diagnosis produced -- see verification summary)" }));
    return stage;
  }
  const ok = diagnosis.diagnosis_status === "DIAGNOSED";
  stage.appendChild(document.createTextNode(diagnosis.diagnosis_status || "UNKNOWN"));
  stage.appendChild(stageStatus(diagnosis.root_cause_category || "", ok));
  if (diagnosis.root_cause) stage.appendChild(el("p", { text: diagnosis.root_cause }));
  if (diagnosis.confidence) stage.appendChild(el("p", { text: `Confidence: ${diagnosis.confidence}` }));
  if (diagnosis.evidence && diagnosis.evidence.length) {
    const evidenceWrap = el("div");
    for (const e of diagnosis.evidence) {
      const item = el("div", { className: "evidence-item" });
      item.appendChild(el("span", { className: "source", text: `[${e.source_type}/${e.source_reference}] ` }));
      item.appendChild(document.createTextNode(e.finding || ""));
      if (e.expected !== null && e.expected !== undefined) {
        item.appendChild(el("div", { text: `expected: ${e.expected}  actual: ${e.actual}` }));
      }
      evidenceWrap.appendChild(item);
    }
    stage.appendChild(evidenceWrap);
  }
  return stage;
}

function renderRepairStage(repairResult, repairPlan) {
  const stage = el("div", { className: "stage" });
  stage.appendChild(el("div", { className: "stage-label", text: "Repair" }));
  if (!repairResult) {
    stage.appendChild(el("p", { text: "(no repair attempted)" }));
    return stage;
  }
  const ok = repairResult.repair_status === "APPLIED";
  stage.appendChild(document.createTextNode(repairResult.repair_status || "UNKNOWN"));
  stage.appendChild(stageStatus(repairResult.repair_type || "", ok));
  if (repairResult.target_file) stage.appendChild(el("p", { text: `Target: ${repairResult.target_file}` }));
  if (repairResult.application_details) stage.appendChild(el("p", { text: repairResult.application_details }));
  const diff = repairPlan && repairPlan.patch && repairPlan.patch.content;
  if (typeof diff === "string") {
    stage.appendChild(el("pre", { className: "diff", text: diff }));
  }
  return stage;
}

function renderVerificationStage(verification) {
  const stage = el("div", { className: "stage" });
  stage.appendChild(el("div", { className: "stage-label", text: "Verification" }));
  const ok = verification.verification_status === "VERIFIED" || verification.verification_status === "VERIFIED_PENDING_PR";
  stage.appendChild(document.createTextNode(verification.verification_status || "UNKNOWN"));
  stage.appendChild(stageStatus(ok ? (verification.verification_status === "VERIFIED" ? "PROMOTED" : "PENDING REVIEW") : "NOT VERIFIED", ok));
  if (verification.tests) {
    stage.appendChild(
      el("p", { text: `Tests: targeted=${verification.tests.targeted}, full=${verification.tests.full_relevant_suite}` })
    );
  }
  if (verification.failed_checks_before || verification.failed_checks_after) {
    stage.appendChild(
      el("p", {
        text: `Failed checks before: [${(verification.failed_checks_before || []).join(", ")}] -> after: [${(verification.failed_checks_after || []).join(", ")}]`,
      })
    );
  }
  if (verification.summary) stage.appendChild(el("p", { text: verification.summary }));
  return stage;
}

function renderPrArtifact(container, prArtifact) {
  clear(container);
  if (!prArtifact) {
    container.appendChild(
      el("p", { className: "muted", text: "No PR-ready review package yet -- run an incident investigation with an approved repair that reaches VERIFIED_PENDING_PR." })
    );
    return;
  }
  const header = el("div", { className: "pr-header" });
  header.appendChild(el("span", { className: `risk-badge ${prArtifact.risk_classification}`, text: `${prArtifact.risk_classification} RISK` }));
  if (prArtifact.human_review_required) header.appendChild(el("span", { className: "chip warn", text: "Human review required" }));
  container.appendChild(header);

  container.appendChild(el("p", { text: `Branch: ${prArtifact.branch || "(none)"}` }));
  container.appendChild(el("p", { text: `Target file: ${prArtifact.target_file}` }));
  if (prArtifact.root_cause_category) container.appendChild(el("p", { text: `Root cause category: ${prArtifact.root_cause_category}` }));
  if (prArtifact.diagnosis_summary) container.appendChild(el("p", { text: prArtifact.diagnosis_summary }));

  container.appendChild(el("h3", { text: "Diff" }));
  container.appendChild(el("pre", { className: "diff", text: prArtifact.diff || "(no diff)" }));

  container.appendChild(el("h3", { text: "Checks" }));
  container.appendChild(el("p", { text: `Failed before: [${(prArtifact.failed_checks_before || []).join(", ")}]` }));
  container.appendChild(el("p", { text: `Failed after: [${(prArtifact.failed_checks_after || []).join(", ")}]` }));
  if (prArtifact.tests_status) {
    container.appendChild(
      el("p", { text: `Tests: targeted=${prArtifact.tests_status.targeted}, full=${prArtifact.tests_status.full_relevant_suite}` })
    );
  }

  container.appendChild(
    el("div", {
      className: "production-unchanged-note",
      text: "Production repository and trusted curated data are unchanged. This is a local, reviewable candidate only -- nothing here has been pushed or promoted.",
    })
  );
}

function findPrArtifact(incidentResult) {
  if (!incidentResult) return null;
  const selfHeal = incidentResult.self_heal;
  if (!selfHeal) return null;
  // pipeline_name path: self_heal is a single heal dict. question path: self_heal maps
  // pipeline_name -> heal dict.
  const heals = selfHeal.repair_verification !== undefined ? [selfHeal] : Object.values(selfHeal);
  for (const heal of heals) {
    const artifact = heal && heal.repair_verification && heal.repair_verification.pr_artifact;
    if (artifact) return artifact;
  }
  return null;
}

// --- Overview tab -------------------------------------------------------------------------

let estateCache = null;

async function fetchEstate() {
  if (estateCache) return estateCache;
  const res = await fetch("/api/estate");
  const data = await res.json();
  if (!res.ok) throw new Error(data.detail || `estate request failed (${res.status})`);
  estateCache = data;
  return data;
}

async function loadOverview() {
  const summaryBox = document.getElementById("overview-summary");
  const scaleBox = document.getElementById("overview-scale");
  try {
    const data = await fetchEstate();
    const rows = data.pipelines || [];
    const trusted = rows.filter((r) => r.validation_status === "PASS").length;
    clear(summaryBox);
    summaryBox.appendChild(el("p", { text: `${trusted} of ${rows.length} data products trusted (passing independent validation).` }));
    const chips = el("div");
    for (const r of rows) {
      const ok = r.validation_status === "PASS";
      chips.appendChild(el("span", { className: `chip${ok ? "" : " warn"}`, text: r.pipeline_name }));
    }
    summaryBox.appendChild(chips);
  } catch (err) {
    summaryBox.textContent = `Could not load estate: ${err.message}`;
  }

  try {
    const res = await fetch("/api/scale");
    if (res.status === 404) {
      scaleBox.textContent = "Scale summary endpoint not available.";
      return;
    }
    const data = await res.json();
    clear(scaleBox);
    scaleBox.appendChild(el("p", { text: `${fmtNumber(data.customers)} customers, ${fmtNumber(data.raw_total_rows)} raw rows across ${Object.keys(data.raw_table_row_counts || {}).length} tables.` }));
    for (const [prefix, stats] of Object.entries(data.storage || {})) {
      scaleBox.appendChild(el("p", { text: `${prefix}/: ${fmtNumber(stats.file_count)} files, ${fmtBytes(stats.total_bytes)}` }));
    }
    scaleBox.appendChild(el("p", { text: `${data.registered_pipelines} registered pipelines, ${data.upstream_services} upstream services.` }));
  } catch (err) {
    scaleBox.textContent = "Scale summary unavailable.";
  }
}

// --- Data Products tab --------------------------------------------------------------------

async function loadDataProducts() {
  const box = document.getElementById("data-products-table");
  try {
    const data = await fetchEstate();
    const rows = data.pipelines || [];
    clear(box);
    const table = el("table");
    table.appendChild(
      el("thead", {
        children: [
          el("tr", {
            children: [
              el("th", { text: "Data product" }),
              el("th", { text: "ETL" }),
              el("th", { text: "Trust" }),
              el("th", { text: "Context" }),
              el("th", { text: "Review" }),
              el("th", { text: "Conflicts" }),
            ],
          }),
        ],
      })
    );
    const tbody = el("tbody");
    for (const r of rows) {
      const trusted = r.validation_status === "PASS";
      tbody.appendChild(
        el("tr", {
          className: trusted ? "" : "estate-row-untrusted",
          children: [
            el("td", { text: r.pipeline_name }),
            el("td", { text: r.etl_status || "-" }),
            el("td", { text: r.validation_status || "UNKNOWN" }),
            el("td", { text: r.context_provenance }),
            el("td", { text: r.review_status || "-" }),
            el("td", { text: String(r.open_conflicts) }),
          ],
        })
      );
    }
    table.appendChild(tbody);
    box.appendChild(table);
  } catch (err) {
    box.textContent = `Could not load data product estate: ${err.message}`;
  }
}

// --- Context tab ----------------------------------------------------------------------

async function populatePipelineSelects() {
  try {
    const data = await fetchEstate();
    const names = (data.pipelines || []).map((r) => r.pipeline_name);
    for (const selectId of ["context-pipeline-select", "incident-pipeline-select"]) {
      const select = document.getElementById(selectId);
      if (!select || select.options.length) continue;
      for (const name of names) select.appendChild(el("option", { text: name, attrs: { value: name } }));
    }
  } catch (err) {
    // Selects stay empty; the tabs that need them will surface their own errors on use.
  }
}

async function loadContextTab() {
  await populatePipelineSelects();
  const select = document.getElementById("context-pipeline-select");
  if (select.value) loadContextDetail(select.value);
}

async function loadContextDetail(pipelineName) {
  const errorBox = document.getElementById("context-error");
  errorBox.classList.add("hidden");
  try {
    const res = await fetch(`/api/context/${encodeURIComponent(pipelineName)}`);
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || `request failed (${res.status})`);

    const metricsBox = document.getElementById("context-metrics");
    clear(metricsBox);
    for (const m of data.metrics || []) {
      const row = el("div", { className: "metric-row" });
      const line = el("div");
      line.appendChild(document.createTextNode(m.metric_name));
      line.appendChild(el("span", { className: "provenance-badge", text: m.provenance }));
      if (m.review_status) line.appendChild(el("span", { className: "provenance-badge", text: m.review_status }));
      row.appendChild(line);
      if (m.conflicts && m.conflicts.length) {
        row.appendChild(el("div", { className: "conflict-note", text: `${m.conflicts.length} open context conflict(s)` }));
      }
      metricsBox.appendChild(row);
    }
    if (!(data.metrics || []).length) metricsBox.appendChild(el("p", { className: "muted", text: "No metric definitions found." }));

    const lineageBox = document.getElementById("context-lineage");
    clear(lineageBox);
    lineageBox.appendChild(el("p", { text: `Provenance: ${data.lineage.provenance}` }));
    lineageBox.appendChild(el("pre", { className: "diff", text: JSON.stringify(data.lineage.value, null, 2) }));

    const metadataBox = document.getElementById("context-pipeline-metadata");
    clear(metadataBox);
    metadataBox.appendChild(el("p", { text: `Provenance: ${data.pipeline_metadata.provenance}` }));
    metadataBox.appendChild(el("pre", { className: "diff", text: JSON.stringify(data.pipeline_metadata.value, null, 2) }));

    const healthBox = document.getElementById("context-health");
    clear(healthBox);
    healthBox.appendChild(el("p", { text: `Provenance: ${data.runtime_health.provenance}` }));
    healthBox.appendChild(el("pre", { className: "diff", text: JSON.stringify(data.runtime_health.value, null, 2) }));
  } catch (err) {
    errorBox.textContent = `Could not load context for ${pipelineName}: ${err.message}`;
    errorBox.classList.remove("hidden");
  }
}

document.getElementById("context-pipeline-select").addEventListener("change", (event) => {
  if (event.target.value) loadContextDetail(event.target.value);
});

// --- Incidents tab -------------------------------------------------------------------------

let lastIncidentResult = null;

function renderIncidentAnswer(result) {
  const box = document.getElementById("incident-answer-block");
  if (result.answer) {
    renderAnswer(box, result.answer);
    if (result.corrected_answer) {
      document.getElementById("incident-candidate-card").className = "card corrected";
      renderAnswer(document.getElementById("incident-candidate-block"), result.corrected_answer);
      document.getElementById("incident-candidate-card").querySelector("h2").textContent = "Corrected (promoted) answer";
    }
  } else {
    // pipeline_name path -- no natural-language answer, just the trust check outcome.
    clear(box);
    const trustworthy = !result.self_heal;
    box.appendChild(
      el("span", { className: `status-badge ${trustworthy ? "ANSWERED" : "UNRELIABLE_DATA"}`, text: trustworthy ? "TRUSTED" : "INCIDENT" })
    );
    box.appendChild(el("p", { text: `${result.pipeline_name}: ${trustworthy ? "trust check passed, no incident." : "trust check failed -- see incident response below."}` }));
  }

  if (result.candidate_answer) {
    document.getElementById("incident-candidate-card").className = "card corrected";
    document.getElementById("incident-candidate-card").querySelector("h2").textContent = "Corrected candidate result (unpromoted)";
    renderAnswer(document.getElementById("incident-candidate-block"), result.candidate_answer);
  } else if (!result.corrected_answer) {
    document.getElementById("incident-candidate-card").className = "card corrected hidden";
  }
}

function renderIncidentSelfHeal(selfHeal) {
  const card = document.getElementById("incident-self-heal-card");
  const container = document.getElementById("incident-self-heal");
  if (!selfHeal) {
    card.className = "card hidden";
    return;
  }
  card.className = "card";
  clear(container);
  // pipeline_name path: a single heal dict (has repair_verification directly).
  // question path: {pipeline_name: heal_dict, ...}.
  const entries = selfHeal.repair_verification !== undefined ? [[null, selfHeal]] : Object.entries(selfHeal);
  for (const [pipelineName, heal] of entries) {
    const block = el("div", { className: "pipeline-block" });
    if (pipelineName) block.appendChild(el("h3", { text: pipelineName }));
    block.appendChild(renderDiagnosisStage(heal.diagnosis));
    block.appendChild(renderRepairStage(heal.repair_result, heal.repair_plan));
    block.appendChild(renderVerificationStage(heal.repair_verification || {}));
    container.appendChild(block);
  }
}

function renderIncidentResult(result) {
  lastIncidentResult = result;

  document.getElementById("incident-relevant-card").className =
    result.relevant_pipelines && result.relevant_pipelines.length ? "card" : "card hidden";
  if (result.relevant_pipelines) {
    const box = document.getElementById("incident-relevant-pipelines");
    clear(box);
    for (const p of result.relevant_pipelines) box.appendChild(el("span", { className: "chip", text: p }));
  }

  const failures = result.validation_failures || {};
  const failuresCard = document.getElementById("incident-validation-failures-card");
  if (Object.keys(failures).length) {
    failuresCard.className = "card";
    const box = document.getElementById("incident-validation-failures");
    clear(box);
    for (const [pipeline, checks] of Object.entries(failures)) {
      const block = el("div", { className: "pipeline-block" });
      block.appendChild(el("h3", { text: pipeline }));
      const ul = el("ul");
      for (const c of checks) ul.appendChild(el("li", { text: c }));
      block.appendChild(ul);
      box.appendChild(block);
    }
  } else {
    failuresCard.className = "card hidden";
  }

  renderIncidentAnswer(result);
  renderIncidentSelfHeal(result.self_heal);

  document.getElementById("incident-results").classList.remove("hidden");

  // Keep the Repairs tab in sync with whatever this incident produced.
  renderPrArtifact(document.getElementById("repairs-content"), findPrArtifact(result));
}

async function runIncident(body) {
  const spinner = document.getElementById("incident-spinner");
  const errorBox = document.getElementById("incident-error");
  const results = document.getElementById("incident-results");
  const buttons = document.querySelectorAll("#incident-question-form button, #incident-pipeline-form button");

  spinner.classList.remove("hidden");
  errorBox.classList.add("hidden");
  results.classList.add("hidden");
  for (const b of buttons) b.disabled = true;

  try {
    const res = await fetch("/api/incident", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (!res.ok) {
      errorBox.textContent = data.detail || `Request failed (${res.status})`;
      errorBox.classList.remove("hidden");
      return;
    }
    renderIncidentResult(data);
    loadHealth();
    estateCache = null; // the estate may have changed (e.g. auto_promote) -- refetch next time
  } catch (err) {
    errorBox.textContent = "Could not reach the API.";
    errorBox.classList.remove("hidden");
  } finally {
    spinner.classList.add("hidden");
    for (const b of buttons) b.disabled = false;
  }
}

document.getElementById("incident-question-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const question = document.getElementById("incident-question-input").value.trim();
  if (question) runIncident({ question, mode: "create_pr" });
});

document.getElementById("incident-pipeline-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const pipelineName = document.getElementById("incident-pipeline-select").value;
  if (!pipelineName) return;
  const mode = document.getElementById("incident-mode-select").value;
  const approveOverride = document.getElementById("approve-source-contract-change").checked;
  runIncident({
    pipeline_name: pipelineName,
    mode,
    approve_categories: approveOverride ? ["SOURCE_CONTRACT_CHANGE"] : [],
  });
});

// --- Evaluations tab ------------------------------------------------------------------

async function loadEvaluations() {
  const box = document.getElementById("evaluations-content");
  try {
    const res = await fetch("/api/evaluations");
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || `request failed (${res.status})`);
    clear(box);
    if (!data.available) {
      box.appendChild(
        el("p", { className: "muted", text: "No eval run recorded yet -- run: python3 -m src.eval_harness" })
      );
      return;
    }
    const summary = data.report.summary || {};
    box.appendChild(el("p", { text: `Scenarios run: ${summary.scenario_count}` }));
    box.appendChild(el("p", { text: `Diagnosis success rate: ${((summary.diagnosis_success_rate || 0) * 100).toFixed(0)}%` }));
    box.appendChild(el("p", { text: `Repair success rate: ${((summary.repair_success_rate || 0) * 100).toFixed(0)}%` }));
    box.appendChild(el("p", { text: `Refusal accuracy: ${((summary.refusal_accuracy || 0) * 100).toFixed(0)}%` }));
    box.appendChild(el("pre", { className: "diff", text: JSON.stringify(data.report, null, 2) }));
  } catch (err) {
    box.textContent = `Could not load evaluations: ${err.message}`;
  }
}

// --- Init -------------------------------------------------------------------------------

loadHealth();
loadOverview();
populatePipelineSelects();
