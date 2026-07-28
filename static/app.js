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
  repairs: loadRepairsTab,
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
    loadRepairsTab();
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

async function loadRepairsTab() {
  const box = document.getElementById("repairs-content");
  try {
    const res = await fetch("/api/repairs/pending");
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || `request failed (${res.status})`);
    clear(box);
    const pending = data.pending || [];
    if (!pending.length) {
      box.appendChild(el("p", { className: "muted", text: "No repairs pending review." }));
      return;
    }
    for (const record of pending) {
      const block = el("div", { className: "pipeline-block" });
      block.appendChild(el("h3", { text: record.pipeline_name }));
      const artifactBox = el("div");
      block.appendChild(artifactBox);
      renderPrArtifact(artifactBox, record.pr_artifact, { pipelineName: record.pipeline_name });
      box.appendChild(block);
    }
  } catch (err) {
    box.textContent = `Could not load pending repairs: ${err.message}`;
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
