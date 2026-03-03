/* CSV Reconciler — app.js (worker-first, stable) */
"use strict";

/* ---------- tiny helpers ---------- */
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
const uid = () => Math.random().toString(36).slice(2, 10);

function clamp(n, a, b){ return Math.max(a, Math.min(b, n)); }

function safeStr(x){ return (x === null || x === undefined) ? "" : String(x); }

function showBanner(kind, title, text = "", ttl = 4500) {
  const host = $("#bannerHost");
  if (!host) return;
  const el = document.createElement("div");
  el.className = `banner ${kind || "info"}`;
  el.innerHTML = `<b>${escapeHtml(title || "")}</b>${text ? `<div class="muted small">${escapeHtml(text)}</div>` : ""}`;
  host.appendChild(el);
  setTimeout(() => {
    el.classList.add("hide");
    setTimeout(() => el.remove(), 250);
  }, ttl);
}

function escapeHtml(s){
  return safeStr(s).replace(/[&<>"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

/* CSV escaping (single source of truth) */
function csvCell(v) {
  const s = safeStr(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, headers){
  const head = (headers || []).map(csvCell).join(",");
  const body = (rows || []).map((r) => (headers || []).map((h) => csvCell(r[h])).join(",")).join("\n");
  return head + "\n" + body + "\n";
}

function downloadText(filename, text, mime="text/plain"){
  const blob = new Blob([text], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

/* ---------- state ---------- */
const state = {
  files: [], // {id,name,file, cols[], delimiter, txidCol, amountCol, statusCol, primary}
  primaryId: null,
  global: {
    tolerance: 0.01,
    delimiter: "",
    keepCols: [],
    txidSearch: ""
  },
  views: { mismatches: true, missing_in_base: true, missing_in_other: true },
  settings: { amountScale: 2, statusMappings: '' },
  pages: new Map(), // pairKey::reportName → { pageSize: 100|200|"all", page: 0 }
  worker: null,
  running: false,
  pairReports: [], // from worker DONE
  expanded: new Map(), // key = pairKey|reportName -> bool
  exportSessions: new Map(), // pairKey|reportName -> {headers, rows[]}
};

/* ---------- DOM refs ---------- */
const els = {
  files: $("#files"),
  results: $("#results"),
  progress: $("#progress"),
  progressBar: $("#progressBar"),
  progressText: $("#progressText"),
  tolerance: $("#tolerance"),
  csvDelimiterGlobal: $("#csvDelimiterGlobal"),
  keepColsGlobal: $("#keepColsGlobal"),
  globalTxidSearch: $("#globalTxidSearch"),
  globalTxidClear: $("#globalTxidClear"),
  filePicker: $("#filePicker"),
  runBtn: $("#runBtn"),
  addFileBtn: $("#addFileBtn"),
  detailsPanel: $("#detailsPanel"),
  detailsBody: $("#detailsBody"),
  detailsCloseBtn: $("#detailsCloseBtn"),
  detailsCopyTxidBtn: $("#detailsCopyTxidBtn"),
  modalBackdrop: $("#modalBackdrop"),
  modal: $("#modal"),
  modalTitle: $("#modalTitle"),
  modalBody: $("#modalBody"),
  modalFoot: $("#modalFoot"),
  modalClose: $("#modalClose"),
  viewsBtn: $("#viewsBtn"),
  colsBtn: $("#colsBtn"),
};

/* ---------- init ---------- */
window.addEventListener("error", (e) => {
  showBanner("warn", "UI error", e?.message || "Unknown error", 9000);
});
window.addEventListener("unhandledrejection", (e) => {
  showBanner("warn", "Promise rejection", safeStr(e?.reason?.message || e?.reason || "Unknown"), 9000);
});

document.addEventListener("DOMContentLoaded", () => {
  bindGlobal();
  renderFiles();
  renderResults();
});

function bindGlobal(){
  // keyboard accessibility for label-button
  els.addFileBtn?.addEventListener("keydown", (ev) => {
    if (ev.key === "Enter" || ev.key === " ") { ev.preventDefault(); els.addFileBtn.click(); }
  });

  // native file picker change
  els.filePicker?.addEventListener("change", async () => {
    const file = els.filePicker.files && els.filePicker.files[0];
    els.filePicker.value = "";
    if (!file) return;

    const f = {
      id: uid(),
      name: (file.name || "file").replace(/\.[^.]+$/, ""),
      file,
      cols: [],
      delimiter: "", // per-file override; "" = use global
      txidCol: "",
      amountCol: "",
      statusCol: "",
      primary: false,
    };
    state.files.push(f);
    if (!state.primaryId) { state.primaryId = f.id; f.primary = true; }
    await refreshHeader(f);
    renderFiles();
  });

  els.tolerance?.addEventListener("change", () => {
    const val = parseFloat(els.tolerance.value);
    if (isNaN(val) || val < 0) {
      els.tolerance.value = String(state.global.tolerance);
      showBanner("warn", "Invalid tolerance", "Must be a non-negative number", 3000);
      return;
    }
    state.global.tolerance = val;
  });
  els.csvDelimiterGlobal?.addEventListener("change", () => {
    state.global.delimiter = decodeDelim(els.csvDelimiterGlobal.value);
    // refresh headers using global delimiter where per-file override is empty
    Promise.all(state.files.filter(f => !f.delimiter).map(refreshHeader)).then(renderFiles);
  });
  els.keepColsGlobal?.addEventListener("change", () => {
    state.global.keepCols = (els.keepColsGlobal.value || "").split(",").map(s => s.trim()).filter(Boolean);
  });
  els.globalTxidSearch?.addEventListener("input", () => {
    state.global.txidSearch = (els.globalTxidSearch.value || "").trim();
    renderResults();
  });
  els.globalTxidClear?.addEventListener("click", () => {
    state.global.txidSearch = "";
    if (els.globalTxidSearch) els.globalTxidSearch.value = "";
    renderResults();
  });

  els.runBtn?.addEventListener("click", run);
  els.viewsBtn?.addEventListener("click", openViewsModal);
  els.colsBtn?.addEventListener("click", openSettingsModal);

  els.detailsCloseBtn?.addEventListener("click", closeDetails);
  els.detailsCopyTxidBtn?.addEventListener("click", () => {
    const txid = els.detailsPanel?.dataset?.txid || "";
    if (!txid) return;
    navigator.clipboard?.writeText(txid);
    showBanner("ok", "Copied", "txid copied", 1500);
  });

  els.modalClose?.addEventListener("click", closeModal);
  els.modalBackdrop?.addEventListener("click", (e) => {
    if (e.target === els.modalBackdrop) closeModal();
  });

  // Results event delegation
  els.results?.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-action]");
    if (!btn) return;
    const act = btn.dataset.action;
    if (act === "toggle") {
      const pairKey = btn.dataset.pair;
      const reportName = btn.dataset.report;
      toggleExpanded(pairKey, reportName);
      return;
    }
    if (act === "export") {
      const pairKey = btn.dataset.pair;
      const reportName = btn.dataset.report;
      exportReport(pairKey, reportName);
      return;
    }
    if (act === "row") {
      const pairKey = btn.dataset.pair;
      const reportName = btn.dataset.report;
      const txid = btn.dataset.txid;
      openRowDetails(pairKey, reportName, txid);
      return;
    }
    if (act === "page-size") {
      const k = `${btn.dataset.pair}::${btn.dataset.report}`;
      const cur = state.pages.get(k) || { pageSize: 100, page: 0 };
      const raw = btn.dataset.size;
      state.pages.set(k, { pageSize: raw === "all" ? "all" : parseInt(raw, 10), page: 0 });
      renderResults();
      return;
    }
    if (act === "page-prev") {
      const k = `${btn.dataset.pair}::${btn.dataset.report}`;
      const cur = state.pages.get(k) || { pageSize: 100, page: 0 };
      state.pages.set(k, { ...cur, page: Math.max(0, cur.page - 1) });
      renderResults();
      return;
    }
    if (act === "page-next") {
      const k = `${btn.dataset.pair}::${btn.dataset.report}`;
      const cur = state.pages.get(k) || { pageSize: 100, page: 0 };
      state.pages.set(k, { ...cur, page: cur.page + 1 });
      renderResults();
      return;
    }
  });
}

function decodeDelim(v){
  if (v === "\\t") return "\t";
  return v || "";
}

/* ---------- header parsing (main thread) ---------- */
async function refreshHeader(f){
  try{
    const text = await f.file.text();
    const delim = f.delimiter || state.global.delimiter || undefined;
    const parsed = Papa.parse(text, {
      header: true,
      preview: 1,
      skipEmptyLines: true,
      ...(delim ? { delimiter: delim } : {})
    });
    f.cols = parsed.meta.fields || [];
    // default guesses
    if (!f.txidCol) f.txidCol = guessCol(f.cols, ["txid","transaction","id","reference"]);
    if (!f.amountCol) f.amountCol = guessCol(f.cols, ["amount","amt","sum","value","total"]);
    if (!f.statusCol) f.statusCol = guessCol(f.cols, ["status","state","result"]);
  } catch(err){
    showBanner("warn", "Header parse failed", err?.message || String(err), 7000);
    f.cols = [];
  }
}

function guessCol(cols, needles){
  const lower = cols.map(c => safeStr(c).toLowerCase());
  for (const n of needles){
    const idx = lower.findIndex(c => c.includes(n));
    if (idx >= 0) return cols[idx];
  }
  return cols[0] || "";
}

/* ---------- render: files ---------- */
function renderFiles(){
  if (!els.files) return;
  if (!state.files.length){
    els.files.innerHTML = `<div class="muted small">No files added yet</div>`;
    return;
  }

  els.files.innerHTML = state.files.map(f => {
    const colOpts = f.cols.map(c => `<option value="${escapeHtml(c)}"${c===f.txidCol?' selected':''}>${escapeHtml(c)}</option>`).join("");
    const amtOpts = f.cols.map(c => `<option value="${escapeHtml(c)}"${c===f.amountCol?' selected':''}>${escapeHtml(c)}</option>`).join("");
    const stOpts  = `<option value="">(none)</option>` + f.cols.map(c => `<option value="${escapeHtml(c)}"${c===f.statusCol?' selected':''}>${escapeHtml(c)}</option>`).join("");

    const delim = f.delimiter || "";
    const delimOpt = (v,label) => `<option value="${escapeHtml(v)}"${v===delim?' selected':''}>${label}</option>`;
    const delimSelect = `
      <select class="mono small" data-file="${f.id}" data-field="delimiter">
        ${delimOpt("", "Use global")}
        ${delimOpt(",", "Comma (,)")}
        ${delimOpt(";", "Semicolon (;)")}
        ${delimOpt("\\t", "Tab (\\t)")}
        ${delimOpt("|", "Pipe (|)")}
      </select>`;

    return `
      <div class="fileRow" data-file="${f.id}">
        <div class="fileMain">
          <label class="radio">
            <input type="radio" name="primary"${f.primary ? " checked":""} data-file="${f.id}" data-field="primary"/>
            <span>PRIMARY</span>
          </label>
          <div class="fileName mono">${escapeHtml(f.name)}</div>
          <button class="ghost small" type="button" data-file="${f.id}" data-field="remove">Remove</button>
        </div>

        <div class="fileGrid">
          <div class="fileField">
            <div class="muted small">CSV delimiter</div>
            ${delimSelect}
          </div>

          <div class="fileField">
            <div class="muted small">TXID column</div>
            <select class="mono small" data-file="${f.id}" data-field="txidCol">${colOpts}</select>
          </div>

          <div class="fileField">
            <div class="muted small">Amount column</div>
            <select class="mono small" data-file="${f.id}" data-field="amountCol">${amtOpts}</select>
          </div>

          <div class="fileField">
            <div class="muted small">Status column</div>
            <select class="mono small" data-file="${f.id}" data-field="statusCol">${stOpts}</select>
          </div>
        </div>

        <div class="fileCols mono small">${f.cols.length ? escapeHtml(f.cols.join(", ")) : "—"}</div>
      </div>
    `;
  }).join("");

  // bind file row controls (delegated within files panel)
  // simplest: bind once per render
  els.files.querySelectorAll("[data-file][data-field]").forEach(el => {
    el.addEventListener("change", async (ev) => {
      const fileId = el.dataset.file;
      const field = el.dataset.field;
      const f = state.files.find(x => x.id === fileId);
      if (!f) return;

      if (field === "primary") {
        state.files.forEach(x => x.primary = false);
        f.primary = true;
        state.primaryId = f.id;
        renderFiles();
        return;
      }

      if (field === "delimiter") {
        f.delimiter = decodeDelim(el.value);
        await refreshHeader(f);
        renderFiles();
        return;
      }

      if (field === "txidCol") f.txidCol = el.value;
      if (field === "amountCol") f.amountCol = el.value;
      if (field === "statusCol") f.statusCol = el.value;
    });
  });

  els.files.querySelectorAll("button[data-field=remove]").forEach(btn => {
    btn.addEventListener("click", () => {
      const fileId = btn.dataset.file;
      state.files = state.files.filter(f => f.id !== fileId);
      if (state.primaryId === fileId) {
        state.primaryId = state.files[0]?.id || null;
        state.files.forEach((f, i) => f.primary = (f.id === state.primaryId));
      }
      renderFiles();
      renderResults();
    });
  });
}

/* ---------- worker plumbing ---------- */
function ensureWorker(){
  if (state.worker) return state.worker;
  const w = new Worker(`./worker.js?v=${Date.now()}`);
  w.onmessage = onWorkerMessage;
  w.onerror = (e) => showBanner("warn", "Worker error", e?.message || "Unknown", 9000);
  state.worker = w;
  return w;
}

function setProgress(text, pct){
  if (els.progress) els.progress.hidden = false;
  if (els.progressText) els.progressText.textContent = text || "Working…";
  if (els.progressBar) els.progressBar.style.width = `${clamp(pct ?? 0, 0, 100)}%`;
}

function clearProgress(){
  if (els.progress) els.progress.hidden = true;
  if (els.progressBar) els.progressBar.style.width = "0%";
  if (els.progressText) els.progressText.textContent = "Working…";
}

function onWorkerMessage(ev){
  const msg = ev.data || {};
  if (!msg.type) return;

  if (msg.type === "STAGE") setProgress(msg.text || "Working…", msg.pct ?? 0);
  if (msg.type === "PROGRESS") setProgress(msg.text || "Working…", msg.pct ?? 0);

  if (msg.type === "DONE") {
    state.running = false;
    clearProgress();
    state.pairReports = msg.pairReports || [];
    showBanner("ok", "Reconciliation complete", "", 2500);
    renderResults();
  }

  if (msg.type === "ERROR") {
    state.running = false;
    clearProgress();
    showBanner("warn", "Worker error", msg.message || safeStr(msg.error || "Unknown"), 9000);
  }

  if (msg.type === "CANCELLED") {
    state.running = false;
    clearProgress();
    showBanner("info", "Cancelled", "Reconciliation was cancelled", 2000);
  }

  if (msg.type === "DETAILS_BATCH_DONE") {
    // handled by awaiting promise map
    const key = `${msg.fileSide}:${msg.fileId}`;
    const resolver = pendingDetails.get(key);
    if (resolver) {
      pendingDetails.delete(key);
      resolver(msg.rows || {});
    }
  }

  if (msg.type === "EXPORT_META") {
    const k = `${msg.pairKey}::${msg.reportName}`;
    state.exportSessions.set(k, { headers: msg.headers || null, rows: [] });
  }
  if (msg.type === "EXPORT_CHUNK") {
    const k = `${msg.pairKey}::${msg.reportName}`;
    const sess = state.exportSessions.get(k);
    if (!sess) return;
    sess.headers = msg.headers || sess.headers;
    (msg.rows || []).forEach(r => sess.rows.push(r));
  }
  if (msg.type === "EXPORT_DONE") {
    const k = `${msg.pairKey}::${msg.reportName}`;
    const sess = state.exportSessions.get(k);
    if (!sess) return;
    const headers = sess.headers || msg.headers || Object.keys(sess.rows[0] || {});
    const csv = toCsv(sess.rows, headers);
    downloadText(`${msg.reportName}_${msg.pairKey}.csv`, csv, "text/csv");
    state.exportSessions.delete(k);
    showBanner("ok", "Export ready", `${msg.reportName} downloaded`, 2500);
  }
}

const pendingDetails = new Map(); // key -> resolver

function askDetails(fileSide, fileId, txids){
  const w = ensureWorker();
  return new Promise((resolve) => {
    const key = `${fileSide}:${fileId}`;
    const timer = setTimeout(() => {
      if (pendingDetails.has(key)) {
        pendingDetails.delete(key);
        resolve({});
      }
    }, 30000);
    pendingDetails.set(key, (rows) => {
      clearTimeout(timer);
      resolve(rows);
    });
    w.postMessage({ type: "DETAILS_BATCH", fileSide, fileId, txids });
  });
}

/* ---------- run ---------- */
function buildGlobalSettings(){
  const amountScale = state.settings.amountScale;           // number of decimal places (0-18)
  const tolFloat = state.global.tolerance || 0;
  // Convert tolerance (float currency units) to scaled integer units: tol=0.01, scale=2 → 1 unit
  const amountTolerance = Math.round(tolFloat * Math.pow(10, amountScale));
  const reportLimit = 50000;
  const statusMappings = state.settings.statusMappings || "";
  return { amountScale, amountTolerance, reportLimit, statusMappings };
}

function buildCfgForFile(f){
  const cols = f.cols || [];
  const txidIdx = cols.indexOf(f.txidCol);
  const amountIdx = cols.indexOf(f.amountCol);
  const statusIdx = f.statusCol ? cols.indexOf(f.statusCol) : -1;

  if (txidIdx < 0) throw new Error(`TXID column not set for ${f.name}`);
  if (amountIdx < 0) throw new Error(`Amount column not set for ${f.name}`);

  // Map keepCols names → column indices for the worker
  const keepColNames = state.global.keepCols || [];
  const keepIdxs = keepColNames.map(col => cols.indexOf(col)).filter(i => i >= 0);

  const delimiter = f.delimiter || state.global.delimiter || "";
  return {
    id: f.id,
    label: f.name,
    name: f.name,
    file: f.file,
    encoding: "utf-8",
    hasHeader: true,
    txidIdx,
    amountIdx,
    statusIdx: statusIdx >= 0 ? statusIdx : -1,
    keepCols: keepColNames,
    keepIdxs,
    decimalComma: false,
    delimiter: delimiter || undefined,
  };
}

async function run(){
  try{
    if (state.running) return;
    if (state.files.length < 2) { showBanner("warn", "Need more files", "Add at least two CSV files", 6000); return; }
    const primary = state.files.find(f => f.primary) || state.files[0];
    if (!primary) { showBanner("warn", "No primary", "Pick a PRIMARY file", 6000); return; }

    const others = state.files.filter(f => f.id !== primary.id);
    if (!others.length) { showBanner("warn", "Need other files", "Add at least one more file", 6000); return; }

    const w = ensureWorker();
    state.running = true;
    setProgress("Starting…", 0);

    const gs = buildGlobalSettings();
    const primaryCfg = buildCfgForFile(primary);
    const otherCfgs = others.map(buildCfgForFile);

    w.postMessage({ type: "RUN", globalSettings: gs, primary: primaryCfg, others: otherCfgs });
  } catch (err){
    state.running = false;
    clearProgress();
    showBanner("warn", "Cannot run", err?.message || String(err), 9000);
  }
}

/* ---------- results UI ---------- */
function getFileNameById(id){
  return state.files.find(f => f.id === id)?.name || id;
}

function toggleExpanded(pairKey, reportName){
  const k = `${pairKey}::${reportName}`;
  const cur = state.expanded.get(k) || false;
  state.expanded.set(k, !cur);
  renderResults();
}

function exportReport(pairKey, reportName){
  const w = ensureWorker();
  showBanner("info", "Export", `Building ${reportName}…`, 2500);
  w.postMessage({ type: "EXPORT", pairKey, reportName });
}

function filterRows(rows){
  const q = (state.global.txidSearch || "").trim();
  if (!q) return rows;
  return rows.filter(r => safeStr(r.txid).includes(q));
}

function renderResults(){
  if (!els.results) return;

  if (!state.pairReports.length){
    els.results.innerHTML = `<div class="muted small">No results yet</div>`;
    return;
  }

  const html = [];
  for (const pr of state.pairReports){
    const baseName = getFileNameById(pr.baseId);
    const otherName = getFileNameById(pr.otherId);

    html.push(`<div class="resultPair">`);
    html.push(`
      <div class="pairHead">
        <div class="pairTitle mono">${escapeHtml(baseName)} ↔ ${escapeHtml(otherName)}</div>
        <div class="pairMeta muted small">${escapeHtml(pr.pairKey)}</div>
      </div>
    `);

    const reports = pr.reports || {};
    const reportOrder = ["mismatches", "missing_in_base", "missing_in_other"].filter(rn => state.views[rn] !== false);
    for (const rn of reportOrder){
      const rep = reports[rn] || {};
      const rows = filterRows(rep.rows || []);
      const k = `${pr.pairKey}::${rn}`;
      const isOpen = state.expanded.get(k) || false;

      html.push(`
        <div class="reportBlock">
          <div class="reportHead">
            <button type="button" class="ghost small" data-action="toggle" data-pair="${escapeHtml(pr.pairKey)}" data-report="${rn}">
              ${escapeHtml(rn)}: ${rows.length}${isOpen ? " ▾" : " ▸"}
            </button>
            <button type="button" class="ghost small" data-action="export" data-pair="${escapeHtml(pr.pairKey)}" data-report="${rn}">
              Export
            </button>
          </div>
      `);

      if (isOpen){
        html.push(renderRowsTable(pr.pairKey, rn, rows));
      }

      html.push(`</div>`);
    }

    html.push(`</div>`);
  }

  els.results.innerHTML = html.join("");
}

function renderRowsTable(pairKey, reportName, rows){
  if (!rows.length) return `<div class="muted small">No rows</div>`;

  const pgKey = `${pairKey}::${reportName}`;
  const pg = state.pages.get(pgKey) || { pageSize: 100, page: 0 };
  const isAll = pg.pageSize === "all";
  const psz = isAll ? rows.length : pg.pageSize;
  const totalPages = isAll ? 1 : Math.ceil(rows.length / psz);
  const curPage = Math.min(pg.page, Math.max(0, totalPages - 1));
  const start = isAll ? 0 : curPage * psz;
  const slice = rows.slice(start, start + psz);

  // Page-size buttons
  const szBtns = [100, 200, "all"].map(sz => {
    const active = (sz === "all") ? isAll : pg.pageSize === sz;
    const cls = active ? "diff-pgsz-btn diff-pgsz-btn--active" : "diff-pgsz-btn";
    const ep = escapeHtml(pairKey);
    return `<button type="button" class="${cls}" data-action="page-size" data-pair="${ep}" data-report="${reportName}" data-size="${sz}">${sz === "all" ? "ALL" : sz}</button>`;
  }).join("");

  // Nav buttons
  const ep = escapeHtml(pairKey);
  const hasPrev = !isAll && curPage > 0;
  const hasNext = !isAll && curPage < totalPages - 1;
  const pageInfo = isAll
    ? `All ${rows.length} rows`
    : `${start + 1}–${Math.min(start + psz, rows.length)} of ${rows.length}`;

  const nav = isAll ? "" : `
    <div class="diff-page-nav">
      <button type="button" class="diff-nav-btn" data-action="page-prev" data-pair="${ep}" data-report="${reportName}"${hasPrev ? "" : " disabled"}>‹</button>
      <button type="button" class="diff-nav-btn" data-action="page-next" data-pair="${ep}" data-report="${reportName}"${hasNext ? "" : " disabled"}>›</button>
    </div>`;

  const pagination = `
    <div class="diff-pagination">
      <div class="diff-page-sizes">
        <span class="muted small" style="margin-right:4px;">Show:</span>${szBtns}
      </div>
      <div style="display:flex;align-items:center;gap:8px;">
        <span class="diff-page-indicator">${pageInfo}</span>${nav}
      </div>
    </div>`;

  // Columns
  const cols = ["txid"];
  const sample = slice[0] || {};
  for (const k of Object.keys(sample)){
    if (k !== "txid" && cols.length < 8) cols.push(k);
  }

  const thead = cols.map(c => `<th class="mono small">${escapeHtml(c)}</th>`).join("");
  const trs = slice.map(r => {
    const txid = safeStr(r.txid);
    const tds = cols.map(c => `<td class="mono small">${escapeHtml(safeStr(r[c] ?? ""))}</td>`).join("");
    return `<tr data-action="row" data-pair="${ep}" data-report="${reportName}" data-txid="${escapeHtml(txid)}" style="cursor:pointer">${tds}</tr>`;
  }).join("");

  return `
    ${pagination}
    <div class="tableWrap">
      <table class="table">
        <thead><tr>${thead}</tr></thead>
        <tbody>${trs}</tbody>
      </table>
    </div>`;
}

/* ---------- details ---------- */
async function openRowDetails(pairKey, reportName, txid){
  try{
    if (!txid) return;
    const [baseId, otherId] = safeStr(pairKey).split("::");
    const [baseRows, otherRows] = await Promise.all([
      askDetails("base", baseId, [txid]),
      askDetails("other", otherId, [txid]),
    ]);

    const base = baseRows[txid] || null;
    const other = otherRows[txid] || null;

    els.detailsPanel.hidden = false;
    els.detailsPanel.dataset.txid = txid;

    const mk = (title, arr) => {
      if (!arr) return `<div class="muted small">${escapeHtml(title)}: not found</div>`;
      const items = arr.map((v,i) => `<div class="mono small"><span class="muted">[${i}]</span> ${escapeHtml(safeStr(v))}</div>`).join("");
      return `<div class="detailBlock"><div class="muted small">${escapeHtml(title)}</div>${items}</div>`;
    };

    els.detailsBody.innerHTML = `
      <div class="mono" style="margin-bottom:8px;"><b>${escapeHtml(txid)}</b></div>
      ${mk("Base row", base)}
      ${mk("Other row", other)}
    `;
  } catch (err){
    showBanner("warn", "Details error", err?.message || String(err), 9000);
  }
}

function closeDetails(){
  if (!els.detailsPanel) return;
  els.detailsPanel.hidden = true;
  els.detailsPanel.dataset.txid = "";
  if (els.detailsBody) els.detailsBody.innerHTML = "";
}

/* ---------- modal (optional) ---------- */
function openModal(title, bodyHtml, footHtml=""){
  if (!els.modalBackdrop || !els.modal) return;
  els.modalTitle.textContent = title || "";
  els.modalBody.innerHTML = bodyHtml || "";
  els.modalFoot.innerHTML = footHtml || "";
  els.modalBackdrop.hidden = false;
  els.modal.hidden = false;
}
function closeModal(){
  if (!els.modalBackdrop || !els.modal) return;
  els.modalBackdrop.hidden = true;
  els.modal.hidden = true;
  els.modalTitle.textContent = "";
  els.modalBody.innerHTML = "";
  els.modalFoot.innerHTML = "";
}

/* ---------- views modal ---------- */
function openViewsModal(){
  const reportNames = ["mismatches", "missing_in_base", "missing_in_other"];
  const checks = reportNames.map(rn => `
    <label style="display:flex;align-items:center;gap:8px;margin-top:8px;cursor:pointer;">
      <input type="checkbox" id="view_${rn}" ${state.views[rn] !== false ? "checked" : ""} />
      <span class="mono small">${escapeHtml(rn)}</span>
    </label>
  `).join("");
  const bodyHtml = `<div class="toolGroup"><div class="muted small">Show report types in results</div>${checks}</div>`;
  const footHtml = `<button type="button" class="primary" id="viewsSaveBtn">Apply</button>`;
  openModal("Views", bodyHtml, footHtml);
  setTimeout(() => {
    document.getElementById("viewsSaveBtn")?.addEventListener("click", () => {
      reportNames.forEach(rn => {
        state.views[rn] = !!(document.getElementById(`view_${rn}`)?.checked);
      });
      closeModal();
      renderResults();
    });
  }, 0);
}

/* ---------- settings modal ---------- */
function openSettingsModal(){
  const s = state.settings;
  const bodyHtml = `
    <div class="toolGroup">
      <label class="muted small" for="cfg_amountScale">Decimal places in amounts (0–18, e.g. 2 for cents)</label>
      <input id="cfg_amountScale" class="mono" type="number" min="0" max="18" value="${s.amountScale}" style="width:120px;" />
    </div>
    <div class="toolGroup" style="margin-top:12px;">
      <label class="muted small" for="cfg_previewCap">Preview row cap (rows shown per table)</label>
      <input id="cfg_previewCap" class="mono" type="number" min="10" max="5000" value="${s.previewCap}" style="width:120px;" />
    </div>
    <div class="toolGroup" style="margin-top:12px;">
      <label class="muted small" for="cfg_statusMappings">Status mappings (raw=canonical, one per line)</label>
      <textarea id="cfg_statusMappings" class="mono" rows="5"
        style="width:100%;resize:vertical;margin-top:4px;"
        placeholder="e.g.&#10;success=completed&#10;ok=completed&#10;fail=failed">${escapeHtml(s.statusMappings)}</textarea>
    </div>
  `;
  const footHtml = `<button type="button" class="primary" id="settingsSaveBtn">Save</button>`;
  openModal("Settings", bodyHtml, footHtml);
  setTimeout(() => {
    document.getElementById("settingsSaveBtn")?.addEventListener("click", () => {
      const scale = parseInt(document.getElementById("cfg_amountScale")?.value || "2", 10);
      const cap   = parseInt(document.getElementById("cfg_previewCap")?.value  || "200", 10);
      const maps  = document.getElementById("cfg_statusMappings")?.value || "";
      if (!Number.isInteger(scale) || scale < 0 || scale > 18) {
        showBanner("warn", "Invalid decimal places", "Must be an integer from 0 to 18", 3000);
        return;
      }
      if (!Number.isInteger(cap) || cap < 10) {
        showBanner("warn", "Invalid cap", "Preview cap must be at least 10", 3000);
        return;
      }
      state.settings.amountScale   = scale;
      state.settings.previewCap    = Math.min(cap, 5000);
      state.settings.statusMappings = maps;
      closeModal();
      showBanner("ok", "Settings saved", "New settings apply on next Run", 2500);
    });
  }, 0);
}
