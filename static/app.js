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
  "run-details": loadRunDetails,
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

// --- Q&A tab: one entry point into the same system, not a separate chatbot ----------------

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

function addQaHistoryEntry(question, result) {
  const history = document.getElementById("qa-history");
  const entry = el("div", { className: "card qa-entry" });
  entry.appendChild(el("div", { className: "qa-question", text: question }));

  const answerBox = el("div");
  entry.appendChild(answerBox);
  if (result.answer) {
    renderAnswer(answerBox, result.answer);
  } else {
    // pipeline_name-shaped result should never come back from a question, but render
    // gracefully instead of crashing if it ever does.
    answerBox.appendChild(el("p", { className: "muted", text: "(no answer returned)" }));
  }

  if (result.relevant_pipelines && result.relevant_pipelines.length) {
    const chips = el("div");
    for (const p of result.relevant_pipelines) chips.appendChild(el("span", { className: "chip", text: p }));
    entry.appendChild(chips);
  }

  if (result.self_heal) {
    const pipelines = Object.keys(result.self_heal);
    const anyPendingPr = Object.values(result.self_heal).some(
      (heal) => heal.repair_verification && heal.repair_verification.verification_status === "VERIFIED_PENDING_PR"
    );
    entry.appendChild(
      el("p", {
        className: "muted",
        text: anyPendingPr
          ? `This question found ${pipelines.join(", ")} untrusted and generated a candidate repair -- see the Repairs tab.`
          : `This question found ${pipelines.join(", ")} untrusted; the data behind it could not be automatically repaired.`,
      })
    );
  }

  if (result.candidate_answer) {
    const candidateBox = el("div", { className: "card corrected" });
    candidateBox.appendChild(el("h2", { text: "Corrected candidate result (unpromoted)" }));
    const inner = el("div");
    candidateBox.appendChild(inner);
    renderAnswer(inner, result.candidate_answer);
    entry.appendChild(candidateBox);
  }

  history.insertBefore(entry, history.firstChild);
}

async function askQuestion(question) {
  const spinner = document.getElementById("qa-spinner");
  const errorBox = document.getElementById("qa-error");
  const button = document.querySelector("#qa-form button");

  spinner.classList.remove("hidden");
  errorBox.classList.add("hidden");
  button.disabled = true;

  try {
    const res = await fetch("/api/incident", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question, mode: "create_pr" }),
    });
    const data = await res.json();
    if (!res.ok) {
      errorBox.textContent = data.detail || `Request failed (${res.status})`;
      errorBox.classList.remove("hidden");
      return;
    }
    addQaHistoryEntry(question, data);
    loadHealth();
    estateCache = null;
    if (data.self_heal) loadRunDetails();
  } catch (err) {
    errorBox.textContent = "Could not reach the API.";
    errorBox.classList.remove("hidden");
  } finally {
    spinner.classList.add("hidden");
    button.disabled = false;
  }
}

document.getElementById("qa-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const input = document.getElementById("qa-question-input");
  const question = input.value.trim();
  if (!question) return;
  askQuestion(question);
  input.value = "";
});

// --- Repairs tab: pending candidates awaiting a human accept/reject decision --------------

async function decideRepair(pipelineName, branch, decision, statusBox) {
  if (decision === "accept") {
    const confirmed = window.confirm(
      `Accept this repair for ${pipelineName}?\n\nThis performs a REAL local "git merge" of ${branch} into your current branch (never pushed to GitHub), then reruns ${pipelineName} for real so the change actually takes effect.`
    );
    if (!confirmed) return;
  }
  statusBox.textContent = decision === "accept" ? "Merging and rerunning..." : "Discarding candidate...";
  try {
    const res = await fetch(`/api/repairs/${decision}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pipeline_name: pipelineName, branch }),
    });
    const data = await res.json();
    if (!res.ok) {
      statusBox.textContent = `Failed: ${data.detail || res.statusText}`;
      return;
    }
    estateCache = null;
    statusBox.textContent = decision === "accept" ? `Accepted -- ${pipelineName} rerun, validation_status=${data.validation_status}.` : "Rejected -- candidate discarded.";
    loadHealth();
    loadRunDetails();
  } catch (err) {
    statusBox.textContent = "Could not reach the API.";
  }
}

function renderPrArtifact(container, prArtifact, opts = {}) {
  clear(container);
  if (!prArtifact) {
    container.appendChild(el("p", { className: "muted", text: "No PR-ready review package yet." }));
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
      text: "Production repository and trusted curated data are unchanged until explicitly accepted below.",
    })
  );

  if (opts.pipelineName && prArtifact.branch) {
    const statusBox = el("p", { className: "muted" });
    const actions = el("div", { className: "inline-form" });
    actions.appendChild(el("button", { text: "Accept (merge for real)", attrs: { type: "button" } }));
    actions.appendChild(el("button", { text: "Reject (discard)", attrs: { type: "button" } }));
    actions.children[0].addEventListener("click", () => decideRepair(opts.pipelineName, prArtifact.branch, "accept", statusBox));
    actions.children[1].addEventListener("click", () => decideRepair(opts.pipelineName, prArtifact.branch, "reject", statusBox));
    container.appendChild(actions);
    container.appendChild(statusBox);
  }
}

function renderRunDetailsEstate(box, rows) {
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
}

function renderRunDetailsWorkflow(cardEl, box, codexRun) {
  if (!codexRun) {
    cardEl.classList.add("hidden");
    return;
  }
  cardEl.classList.remove("hidden");
  clear(box);
  box.appendChild(el("p", { text: `run_id: ${codexRun.run_id}  --  backend: ${codexRun.backend || "current"}` }));

  const stages = codexRun.stages || [];
  if (stages.length) {
    const ol = el("ol", { className: "mcp-timeline" });
    for (const stage of stages) {
      const label = stage.tool ? `${stage.tool}(${JSON.stringify(stage.arguments || {})})` : JSON.stringify(stage);
      ol.appendChild(el("li", { text: label }));
    }
    box.appendChild(ol);
  }

  if (codexRun.final_report) {
    box.appendChild(el("p", { text: codexRun.final_report.summary || "" }));
  }
}

// Tools whose result carries real Spark/pod runtime evidence -- see
// src/mcp_servers/spark_runtime_server.py. Pulls the LAST matching stage out of the same
// codexRun.stages data renderRunDetailsWorkflow already renders (no new backend call).
const SPARK_EVIDENCE_TOOLS = new Set(["get_spark_application_status", "get_spark_run_summary", "get_pod_status"]);

function renderRunDetailsInfra(cardEl, box, codexRun, historyServerUrl) {
  const stages = (codexRun && codexRun.stages) || [];
  const lastEvidenceStage = [...stages].reverse().find((s) => SPARK_EVIDENCE_TOOLS.has(s.tool));

  if (!historyServerUrl && !lastEvidenceStage) {
    cardEl.classList.add("hidden");
    return;
  }
  cardEl.classList.remove("hidden");
  clear(box);

  if (historyServerUrl) {
    const link = el("a", { text: "View in Spark History Server" });
    link.href = historyServerUrl;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    box.appendChild(el("p", { children: [link] }));
  }

  if (lastEvidenceStage) {
    box.appendChild(el("p", { className: "muted", text: `Latest runtime evidence -- ${lastEvidenceStage.tool}:` }));
    box.appendChild(el("pre", { className: "diff", text: lastEvidenceStage.result }));
  }
}

async function loadRunDetails() {
  const estateBox = document.getElementById("run-details-estate");
  const workflowCard = document.getElementById("run-details-workflow-card");
  const workflowBox = document.getElementById("run-details-workflow");
  const infraCard = document.getElementById("run-details-infra-card");
  const infraBox = document.getElementById("run-details-infra");
  const repairsBox = document.getElementById("run-details-repairs");
  try {
    const res = await fetch("/api/run-details/latest");
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || `request failed (${res.status})`);

    renderRunDetailsEstate(estateBox, data.data_products || []);
    renderRunDetailsWorkflow(workflowCard, workflowBox, data.codex_run);
    renderRunDetailsInfra(infraCard, infraBox, data.codex_run, data.history_server_url);

    clear(repairsBox);
    const pending = data.pending_repairs || [];
    if (!pending.length) {
      repairsBox.appendChild(el("p", { className: "muted", text: "No repairs pending review." }));
    } else {
      for (const record of pending) {
        const block = el("div", { className: "pipeline-block" });
        block.appendChild(el("h3", { text: record.pipeline_name }));
        const artifactBox = el("div");
        block.appendChild(artifactBox);
        renderPrArtifact(artifactBox, record.pr_artifact, { pipelineName: record.pipeline_name });
        repairsBox.appendChild(block);
      }
    }
  } catch (err) {
    estateBox.textContent = `Could not load run details: ${err.message}`;
  }
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

// --- Automatic health-monitor scan ---------------------------------------------------------
//
// Health monitor detects an untrusted data product -> incident created automatically ->
// diagnosed -> a bounded repair is generated automatically (for SOURCE_CONTRACT_CHANGE only
// -- see src.data_ops.AUTO_APPROVED_CATEGORIES) -> Spark reruns against isolated candidate
// data -> validators/tests run -> VERIFIED_PENDING_PR -> a human accepts or rejects (Repairs
// tab). Polling is safe to leave running: a pipeline with an already-pending candidate is
// reported, not re-diagnosed, so repeated polling only spends a real model call once per
// newly-detected incident, not once per poll tick.

const AUTO_SCAN_INTERVAL_MS = 25000;

function _visibleTabName() {
  const visible = document.querySelector(".tab-panel:not(.hidden)");
  return visible ? visible.id.replace("tab-", "") : null;
}

let _autoScanInFlight = false;

async function runAutoScan() {
  if (_autoScanInFlight) return; // a scan (e.g. diagnosing a real incident) can take minutes --
  _autoScanInFlight = true; // never let a 25s poll tick overlap a still-running one.
  const banner = document.getElementById("auto-scan-banner");
  try {
    const useScriptedModel = document.getElementById("use-scripted-model").checked;
    const res = await fetch("/api/incidents/scan", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ use_scripted_model: useScriptedModel }),
    });
    const data = await res.json();
    if (!res.ok) {
      banner.textContent = `Health monitor check failed: ${data.detail || res.statusText}`;
      banner.className = "banner bad";
      return;
    }
    const results = data.results || [];
    const newlyPending = results.filter((r) => r.status === "pending_review");

    estateCache = null;
    loadHealth();
    const visibleTab = _visibleTabName();
    if (visibleTab && TAB_LOADERS[visibleTab]) TAB_LOADERS[visibleTab]();

    if (newlyPending.length) {
      banner.textContent = `Health monitor: ${newlyPending.length} new candidate repair(s) generated -- review in the Repairs tab.`;
      banner.className = "banner bad";
    } else if (results.length) {
      banner.textContent = `Health monitor: ${results.length} data product(s) need attention.`;
      banner.className = "banner bad";
    } else {
      banner.textContent = "Health monitor: all data products trusted.";
      banner.className = "banner ok";
    }
  } catch (err) {
    banner.textContent = "Health monitor: could not reach the API.";
    banner.className = "banner bad";
  } finally {
    _autoScanInFlight = false;
  }
}

// --- Init -------------------------------------------------------------------------------

loadHealth();
loadOverview();
runAutoScan();
setInterval(runAutoScan, AUTO_SCAN_INTERVAL_MS);
