// Lite frontend for src/api.py. Vanilla JS, no build step, no framework -- deliberately
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

function renderRelevantPipelines(container, pipelines) {
  clear(container);
  if (!pipelines || !pipelines.length) {
    container.appendChild(el("p", { text: "None -- the question was answered without touching curated pipeline data." }));
    return;
  }
  for (const p of pipelines) container.appendChild(el("span", { className: "chip", text: p }));
}

function renderValidationFailures(card, container, failures) {
  const entries = Object.entries(failures || {});
  if (!entries.length) {
    card.className = "card hidden";
    return;
  }
  card.className = "card";
  clear(container);
  for (const [pipeline, checks] of entries) {
    const block = el("div", { className: "pipeline-block" });
    block.appendChild(el("h3", { text: pipeline }));
    if (checks.length) {
      const ul = el("ul");
      for (const c of checks) ul.appendChild(el("li", { text: c }));
      block.appendChild(ul);
    } else {
      block.appendChild(el("p", { text: "(no specific check IDs reported)" }));
    }
    container.appendChild(block);
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
  const ok = verification.verification_status === "VERIFIED";
  stage.appendChild(document.createTextNode(verification.verification_status || "UNKNOWN"));
  stage.appendChild(stageStatus(ok ? "PROMOTED" : "NOT PROMOTED", ok));
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

function renderSelfHeal(card, container, selfHeal) {
  if (!selfHeal) {
    card.className = "card hidden";
    return;
  }
  card.className = "card";
  clear(container);
  for (const [pipeline, heal] of Object.entries(selfHeal)) {
    const block = el("div", { className: "pipeline-block" });
    block.appendChild(el("h3", { text: pipeline }));
    block.appendChild(renderDiagnosisStage(heal.diagnosis));
    block.appendChild(renderRepairStage(heal.repair_result, heal.repair_plan));
    block.appendChild(renderVerificationStage(heal.repair_verification));
    container.appendChild(block);
  }
}

async function submitQuestion(question) {
  const spinner = document.getElementById("spinner");
  const errorBox = document.getElementById("error");
  const results = document.getElementById("results");
  const askButton = document.getElementById("ask-button");

  spinner.classList.remove("hidden");
  errorBox.classList.add("hidden");
  results.classList.add("hidden");
  askButton.disabled = true;

  try {
    const res = await fetch("/api/ask", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question }),
    });
    const data = await res.json();
    if (!res.ok) {
      errorBox.textContent = data.detail || `Request failed (${res.status})`;
      errorBox.classList.remove("hidden");
      return;
    }

    document.getElementById("question-echo").textContent = data.question;
    renderAnswer(document.getElementById("answer-block"), data.answer);
    renderRelevantPipelines(document.getElementById("relevant-pipelines"), data.relevant_pipelines);
    renderValidationFailures(
      document.getElementById("validation-failures-card"),
      document.getElementById("validation-failures"),
      data.validation_failures
    );
    renderSelfHeal(document.getElementById("self-heal-card"), document.getElementById("self-heal"), data.self_heal);

    const correctedCard = document.getElementById("corrected-answer-card");
    if (data.corrected_answer) {
      correctedCard.className = "card corrected";
      renderAnswer(document.getElementById("corrected-answer-block"), data.corrected_answer);
    } else {
      correctedCard.className = "card corrected hidden";
    }

    results.classList.remove("hidden");
    loadHealth();
  } catch (err) {
    errorBox.textContent = "Could not reach the API.";
    errorBox.classList.remove("hidden");
  } finally {
    spinner.classList.add("hidden");
    askButton.disabled = false;
  }
}

document.getElementById("ask-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const question = document.getElementById("question-input").value.trim();
  if (question) submitQuestion(question);
});

loadHealth();
