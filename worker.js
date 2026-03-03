/* global Papa */
// Web Worker: streaming parse + index-only reconciliation (iteration 3.4)

// PapaParse is not available in workers by default.
// In GitHub Pages, we can import it from CDN.
importScripts('https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js');
importScripts('./shared.js');

const SAFE_UI_CAP = 1000000;   // max rows per report sent to UI
const KEEP_PREVIEW_CAP = 20000; // max unique txids to keep keepCols preview for
const EXPORT_CHUNK_ROWS = 20000;

// 3.9: columnar/typed storage for unique records
const I64_MIN = -(1n << 63n);
const I64_MAX = (1n << 63n) - 1n;

function makeStatusIntern() {
  const map = new Map();
  const rev = ['']; // code 0 = empty
  return {
    code(s) {
      if (!s) return 0;
      const v = map.get(s);
      if (v != null) return v;
      const id = rev.length;
      rev.push(s);
      map.set(s, id);
      return id;
    },
    str(code) {
      return rev[code] || '';
    },
  };
}

function buildStatusMap(lines) {
  const map = new Map();
  for (const line of (lines || '').split(/\r?\n/)) {
    const t = line.trim();
    if (!t) continue;
    const m = t.split('=');
    if (m.length < 2) continue;
    const raw = m[0].trim().toLowerCase();
    const canon = m.slice(1).join('=').trim().toLowerCase();
    if (raw) map.set(raw, canon);
  }
  return map;
}

function post(type, payload = {}) {
  self.postMessage({ type, ...payload });
}

function parseCounts(file, cfg, stageBasePct, stageSpanPct, abortSignal) {
  return new Promise((resolve, reject) => {
    const counts = new Map();
    let rows = 0;

    const txIdx = cfg.txidIdx;
    Papa.parse(file, {
      header: false,
      skipEmptyLines: true,
      encoding: cfg.encoding,
      ...(cfg.delimiter ? { delimiter: cfg.delimiter } : {}),
      chunkSize: 1024 * 512,
      chunk: (results, parser) => {
        if (abortSignal.aborted) {
          parser.abort();
          return;
        }
        const data = results.data || [];
        for (const row of data) {
          // Skip header row (first non-empty chunk's first row)
          if (rows === 0 && cfg.hasHeader) {
            rows++;
            continue;
          }
          rows++;
          const txid = (row[txIdx] ?? '').toString().trim();
          if (!txid) continue;
          counts.set(txid, (counts.get(txid) || 0) + 1);
        }
        const cursor = results.meta && typeof results.meta.cursor === 'number' ? results.meta.cursor : null;
        if (cursor != null && file.size) {
          const pct = stageBasePct + stageSpanPct * (cursor / file.size);
          post('PROGRESS', { text: cfg.label + ': counting…', pct: Math.min(100, pct) });
        }
      },
      complete: () => {
        const dupRows = [];
        let dupRowsTotal = 0;
        let dupTxids = 0;
        for (const [txid, c] of counts.entries()) {
          if (c > 1) {
            dupTxids++;
            dupRowsTotal += (c - 1);
            if (dupRows.length < SAFE_UI_CAP) {
              dupRows.push({ txid, count: c });
            }
          }
        }
        dupRows.sort((a, b) => (a.txid < b.txid ? -1 : a.txid > b.txid ? 1 : 0));
        resolve({
          counts,
          rowsTotal: rows,
          duplicatesRowsTotal: dupRowsTotal,
          duplicatesTxids: dupTxids,
          dupRows,
        });
      },
      error: (err) => reject(err),
    });
  });
}

function buildSortedUniqueKeys(counts) {
  const keys = [];
  for (const [k, c] of counts.entries()) {
    if (c === 1) keys.push(k);
  }
  keys.sort();
  return keys;
}

// 3.10b: index uniques without keyIndex Map and without per-row binary search.
// Buffer (txid, parsed fields) for count===1, sort once by txid, then linear-merge
// into the already-sorted uniqueKeys[] to fill columnar arrays.
function parseUniqueColumns(file, cfg, counts, uniqueKeys, statusMap, amountScale, statusIntern, abortSignal, stageBasePct, stageSpanPct) {
  return new Promise((resolve, reject) => {
    const keepPreview = new Map();
    let previewCount = 0;

    // Columnar arrays aligned with uniqueKeys[]
    let amount64 = new BigInt64Array(uniqueKeys.length);
    let amountBig = null; // BigInt[] fallback
    const amountMask = new Uint8Array(uniqueKeys.length); // 1 = has amount

    const statusCode = new Uint32Array(uniqueKeys.length); // 0 = empty
    const statusRawArr = new Array(uniqueKeys.length).fill('');

    // Temp buffers for unique rows
    const tTxid = [];
    const tAmt = []; // BigInt|null
    const tStCode = []; // number
    const tStRaw = []; // string

    let line = 0;
    const txIdx = cfg.txidIdx;
    const amtIdx = cfg.amountIdx;
    const stIdx = cfg.statusIdx;
    const keepIdxs = cfg.keepIdxs || [];

    Papa.parse(file, {
      header: false,
      skipEmptyLines: true,
      encoding: cfg.encoding,
      ...(cfg.delimiter ? { delimiter: cfg.delimiter } : {}),
      chunkSize: 1024 * 512,
      chunk: (results, parser) => {
        if (abortSignal.aborted) {
          parser.abort();
          return;
        }
        const data = results.data || [];
        for (const row of data) {
          if (line === 0 && cfg.hasHeader) {
            line++;
            continue;
          }
          line++;

          const txid = (row[txIdx] ?? '').toString().trim();
          if (!txid) continue;
          if (counts.get(txid) !== 1) continue;

          const amountRaw = amtIdx >= 0 ? (row[amtIdx] ?? '').toString() : '';
          const statusRaw = stIdx >= 0 ? (row[stIdx] ?? '').toString() : '';

          const amountScaled = amtIdx >= 0 ? parseAmountScaled(amountRaw, amountScale, cfg.decimalComma) : null;
          const statusNorm = normalizeStatus(statusRaw, statusMap);

          tTxid.push(txid);
          tAmt.push(amountScaled);
          tStCode.push(statusIntern.code(statusNorm || ''));
          tStRaw.push(statusRaw);

          if (keepIdxs.length && previewCount < KEEP_PREVIEW_CAP) {
            const kv = keepIdxs.map((i) => (row[i] ?? '').toString());
            keepPreview.set(txid, kv);
            previewCount++;
          }
        }
        const cursor = results.meta && typeof results.meta.cursor === 'number' ? results.meta.cursor : null;
        if (cursor != null && file.size) {
          const pct = stageBasePct + stageSpanPct * (cursor / file.size);
          post('PROGRESS', { text: cfg.label + ': indexing uniques…', pct: Math.min(100, pct) });
        }
      },
      complete: () => {
        const n = tTxid.length;
        const order = new Array(n);
        for (let i = 0; i < n; i++) order[i] = i;
        order.sort((ia, ib) => {
          const a = tTxid[ia];
          const b = tTxid[ib];
          if (a === b) return 0;
          return a < b ? -1 : 1;
        });

        let p = 0;
        for (let oi = 0; oi < n; oi++) {
          const idx = order[oi];
          const txid = tTxid[idx];

          while (p < uniqueKeys.length && uniqueKeys[p] < txid) p++;
          if (p >= uniqueKeys.length) break;
          if (uniqueKeys[p] !== txid) continue;

          const amountScaled = tAmt[idx];
          if (amountScaled != null) {
            amountMask[p] = 1;
            if (amountBig) {
              amountBig[p] = amountScaled;
            } else {
              if (amountScaled < I64_MIN || amountScaled > I64_MAX) {
                amountBig = new Array(uniqueKeys.length);
                for (let k = 0; k < uniqueKeys.length; k++) amountBig[k] = BigInt(amount64[k]);
                amountBig[p] = amountScaled;
              } else {
                amount64[p] = amountScaled;
              }
            }
          }

          statusCode[p] = tStCode[idx] >>> 0;
          statusRawArr[p] = tStRaw[idx] || '';
        }

        resolve({ amount64, amountBig, amountMask, statusCode, statusRaw: statusRawArr, keepPreview });
      },
      error: (err) => reject(err),
    });
  });
}

function getAmountAt(pack, pos) {
  if (!pack.amountMask[pos]) return null;
  return pack.amountBig ? pack.amountBig[pos] : BigInt(pack.amount64[pos]);
}

function buildDiffReports(base, other, gs) {
  const tol = BigInt(gs.amountTolerance || 0);
  let totalDeltaScaled = 0n;

  const baseKeys = base.uniqueKeys;
  const otherKeys = other.uniqueKeys;

  const reports = {
    missing_in_base: { total: 0, rows: [] },
    missing_in_other: { total: 0, rows: [] },
    mismatches: { total: 0, rows: [] },
    duplicates_base: {
      total: (base.dupRows || []).length,
      rows: (base.dupRows || []).slice(0, SAFE_UI_CAP),
    },
    duplicates_other: {
      total: (other.dupRows || []).length,
      rows: (other.dupRows || []).slice(0, SAFE_UI_CAP),
    },
  };

  let i = 0;
  let j = 0;

  while (i < baseKeys.length || j < otherKeys.length) {
    const bk = i < baseKeys.length ? baseKeys[i] : null;
    const ok = j < otherKeys.length ? otherKeys[j] : null;

    if (bk != null && (ok == null || bk < ok)) {
      // present in base, missing in other
      reports.missing_in_other.total++;
      if (reports.missing_in_other.rows.length < SAFE_UI_CAP) {
        const pos = i;
        const bAmt = getAmountAt(base, pos);
        reports.missing_in_other.rows.push({
          txid: bk,
          mismatch_type: 'missing_in_other',
          base_amount_scaled: bAmt == null ? null : bAmt.toString(),
          other_amount_scaled: null,
          base_status: (base.statusRaw[pos] || ''),
          other_status: '',
          keep_base: base.keepPreview.get(bk) || null,
        });
      }
      i++;
      continue;
    }

    if (ok != null && (bk == null || ok < bk)) {
      // present in other, missing in base
      reports.missing_in_base.total++;
      if (reports.missing_in_base.rows.length < SAFE_UI_CAP) {
        const pos = j;
        const oAmt = getAmountAt(other, pos);
        reports.missing_in_base.rows.push({
          txid: ok,
          mismatch_type: 'missing_in_base',
          base_amount_scaled: null,
          other_amount_scaled: oAmt == null ? null : oAmt.toString(),
          base_status: '',
          other_status: (other.statusRaw[pos] || ''),
          keep_other: other.keepPreview.get(ok) || null,
        });
      }
      j++;
      continue;
    }

    // equal keys
    const k = bk;

    const bp = i;
    const op = j;

    const bAmtV = getAmountAt(base, bp);
    const oAmtV = getAmountAt(other, op);
    const bAmt = bAmtV == null ? null : bAmtV.toString();
    const oAmt = oAmtV == null ? null : oAmtV.toString();

    const bStatCode = base.statusCode[bp];
    const oStatCode = other.statusCode[op];

    const amountParseError = (bAmtV == null) !== (oAmtV == null);
    let amountMismatch = false;
    if (!amountParseError && bAmtV != null && oAmtV != null) {
      const bb = bAmtV;
      const oo = oAmtV;
      const diff = bb > oo ? (bb - oo) : (oo - bb);
      amountMismatch = diff > tol;
    }

    const statusMismatch = bStatCode !== oStatCode;

    if (amountParseError || amountMismatch || statusMismatch) {
      reports.mismatches.total++;
      if (reports.mismatches.rows.length < SAFE_UI_CAP) {
        let mismatch_type = 'mismatch';
        if (amountParseError) mismatch_type = 'amount_missing_one_side';
        else if (amountMismatch && statusMismatch) mismatch_type = 'amount_and_status_mismatch';
        else if (amountMismatch) mismatch_type = 'amount_mismatch';
        else if (statusMismatch) mismatch_type = 'status_mismatch';

        reports.mismatches.rows.push({
          txid: k,
          mismatch_type,
          base_amount_scaled: bAmt,
          other_amount_scaled: oAmt,
          base_status: (base.statusRaw[bp] || ''),
          other_status: (other.statusRaw[op] || ''),
          keep_base: base.keepPreview.get(k) || null,
          keep_other: other.keepPreview.get(k) || null,
        });
      }
      if (bAmtV != null && oAmtV != null) {
        totalDeltaScaled += (bAmtV - oAmtV);
      }
    }

    i++;
    j++;
  }

  reports._meta = {
    base_unique_count: baseKeys.length,
    other_unique_count: otherKeys.length,
    total_delta_scaled: totalDeltaScaled.toString(),
  };

  return reports;
}

async function detailsBatch(file, cfg, txids, abortSignal) {
  const want = new Set(txids);
  const out = new Map();
  let line = 0;
  return new Promise((resolve, reject) => {
    Papa.parse(file, {
      header: false,
      skipEmptyLines: true,
      encoding: cfg.encoding,
      ...(cfg.delimiter ? { delimiter: cfg.delimiter } : {}),
      chunkSize: 1024 * 512,
      chunk: (results, parser) => {
        if (abortSignal.aborted) {
          parser.abort();
          return;
        }
        for (const row of (results.data || [])) {
          if (line === 0 && cfg.hasHeader) {
            line++;
            continue;
          }
          line++;
          const txid = (row[cfg.txidIdx] ?? '').toString().trim();
          if (!txid || !want.has(txid)) continue;
          const kv = (cfg.keepIdxs || []).map((i) => (row[i] ?? '').toString());
          out.set(txid, kv);
          want.delete(txid);
          if (want.size === 0) {
            parser.abort();
            break;
          }
        }
      },
      complete: () => resolve(out),
      error: (err) => reject(err),
    });
  });
}

// In-memory run state (kept inside worker between messages)
const runState = {
  base: null,
  others: [],
  gs: null,
  statusMap: null,
  pairReports: [], // [{pairKey, reports, keepCols}]
  abort: { aborted: false },
};

self.onmessage = async (ev) => {
  const msg = ev.data || {};

  try {
    if (msg.type === 'CANCEL') {
      runState.abort.aborted = true;
      post('CANCELLED');
      return;
    }

    if (msg.type === 'RUN') {
      runState.abort = { aborted: false };
      const abortSignal = runState.abort;

      const gs = msg.globalSettings;
      runState.gs = gs;
      runState.statusMap = buildStatusMap(gs.statusMappings || '');
      const statusIntern = makeStatusIntern();

      post('STAGE', { text: 'Counting txids (pass 1)…', pct: 1 });

      // base pass 1
      const baseCfg = msg.primary;
      const baseCounts = await parseCounts(baseCfg.file, baseCfg, 0, 20, abortSignal);

      // other pass 1
      const others = [];
      const otherCfgs = msg.others || [];
      for (let i = 0; i < otherCfgs.length; i++) {
        const c = otherCfgs[i];
        const basePct = 20 + (i * 20) / Math.max(1, otherCfgs.length);
        const spanPct = 20 / Math.max(1, otherCfgs.length);
        const r = await parseCounts(c.file, c, basePct, spanPct, abortSignal);
        others.push({ cfg: c, counts: r.counts, rowsTotal: r.rowsTotal, duplicatesRowsTotal: r.duplicatesRowsTotal, duplicatesTxids: r.duplicatesTxids, dupRows: r.dupRows || [] });
      }

      if (abortSignal.aborted) return;

      post('STAGE', { text: 'Indexing uniques (pass 2)…', pct: 40 });

      // base uniques
      const baseUniqueKeys = buildSortedUniqueKeys(baseCounts.counts);
      const baseCols = await parseUniqueColumns(
        baseCfg.file,
        baseCfg,
        baseCounts.counts,
        baseUniqueKeys,
        runState.statusMap,
        gs.amountScale,
        statusIntern,
        abortSignal,
        40,
        15
      );

      const basePack = {
        cfg: baseCfg,
        counts: baseCounts.counts,
        rowsTotal: baseCounts.rowsTotal,
        duplicatesRowsTotal: baseCounts.duplicatesRowsTotal,
        duplicatesTxids: baseCounts.duplicatesTxids,
        dupRows: baseCounts.dupRows || [],
        uniqueKeys: baseUniqueKeys,
        amount64: baseCols.amount64,
        amountBig: baseCols.amountBig,
        amountMask: baseCols.amountMask,
        statusCode: baseCols.statusCode,
        statusRaw: baseCols.statusRaw,
        keepPreview: baseCols.keepPreview,
        statusIntern,
      };

      // other pass 2
      for (let i = 0; i < others.length; i++) {
        const o = others[i];
        const basePct = 55 + (i * 20) / Math.max(1, others.length);
        const spanPct = 20 / Math.max(1, others.length);
        o.uniqueKeys = buildSortedUniqueKeys(o.counts);
        const cols = await parseUniqueColumns(
          o.cfg.file,
          o.cfg,
          o.counts,
          o.uniqueKeys,
          runState.statusMap,
          gs.amountScale,
          statusIntern,
          abortSignal,
          basePct,
          spanPct
        );
        o.amount64 = cols.amount64;
        o.amountBig = cols.amountBig;
        o.amountMask = cols.amountMask;
        o.statusCode = cols.statusCode;
        o.statusRaw = cols.statusRaw;
        o.keepPreview = cols.keepPreview;
        o.statusIntern = statusIntern;
      }

      if (abortSignal.aborted) return;

      // Build per-pair reports (UI samples)
      post('STAGE', { text: 'Reconciling…', pct: 80 });

      const pairReports = [];
      for (const o of others) {
        const reports = buildDiffReports(basePack, o, gs);
        pairReports.push({
          pairKey: baseCfg.id + '::' + o.cfg.id,
          baseId: baseCfg.id,
          otherId: o.cfg.id,
          duplicates_base_rows: basePack.duplicatesRowsTotal || 0,
          duplicates_other_rows: o.duplicatesRowsTotal || 0,
          duplicates_base_txids: basePack.duplicatesTxids || 0,
          duplicates_other_txids: o.duplicatesTxids || 0,
          keepCols: baseCfg.keepCols || [],
          reports,
        });
      }

      runState.base = basePack;
      runState.others = others;
      runState.pairReports = pairReports;

      post('DONE', {
        pairReports,
        meta: {
          safeUiCap: SAFE_UI_CAP,
          keepPreviewCap: KEEP_PREVIEW_CAP,
        },
      });
      return;
    }

    if (msg.type === 'DETAILS_BATCH') {
      const abortSignal = runState.abort;
      const { fileSide, fileId, txids } = msg;
      if (!txids || !txids.length) {
        post('DETAILS_BATCH_DONE', { fileId, fileSide, rows: {} });
        return;
      }

      // Find file config
      let cfg = null;
      if (runState.gs && runState.base && runState.base.cfg && runState.base.cfg.id === fileId) {
        cfg = runState.base.cfg;
      } else {
        for (const o of runState.others || []) {
          if (o.cfg && o.cfg.id === fileId) cfg = o.cfg;
        }
      }
      if (!cfg) throw new Error('Unknown file for DETAILS_BATCH');

      const file = cfg.file;
      const map = await detailsBatch(file, cfg, txids, abortSignal);
      const rowsObj = {};
      for (const [k, v] of map.entries()) rowsObj[k] = v;
      post('DETAILS_BATCH_DONE', { fileId, fileSide, rows: rowsObj });
      return;
    }

    if (msg.type === 'EXPORT') {
      const abortSignal = runState.abort;
      const { pairKey, reportName } = msg;
      const pr = (runState.pairReports || []).find((p) => p.pairKey === pairKey);
      if (!pr) throw new Error('Unknown pairKey');

      // For 3.4 export, we regenerate by running merge scan again, but only emit rows for the requested report.
      // This keeps memory bounded.
      const basePack = runState.base;
      const otherPack = (runState.others || []).find((o) => (basePack.cfg.id + '::' + o.cfg.id) === pairKey);
      if (!otherPack) throw new Error('Unknown other pack');

      if (reportName === 'duplicates_base' || reportName === 'duplicates_other') {
        const pack = reportName === 'duplicates_base' ? basePack : otherPack;
        const rows = (pack.dupRows || []);
        const headers = ['txid', 'count'];
        post('EXPORT_META', { pairKey, reportName, headers });
        if (rows.length) post('EXPORT_CHUNK', { pairKey, reportName, headers, rows });
        post('EXPORT_DONE', { pairKey, reportName, headers });
        return;
      }

      // Send headers up-front so UI can build a non-empty CSV even when there are 0 rows.
      // (Previously headers were only sent with EXPORT_CHUNK, so empty reports downloaded as blank files.)
      const keepColHeaders = (pr.keepCols || []).map(c => 'keep_' + c);
      const headers = ['txid', 'mismatch_type', 'base_amount_scaled', 'other_amount_scaled', 'base_status', 'other_status', ...keepColHeaders];

      post('EXPORT_META', { pairKey, reportName, headers });

      const tol = BigInt(runState.gs.amountTolerance || 0);
      const baseKeys = basePack.uniqueKeys;
      const otherKeys = otherPack.uniqueKeys;


      const chunk = [];
      const pushRow = (row) => {
        chunk.push(row);
        if (chunk.length >= EXPORT_CHUNK_ROWS) {
          post('EXPORT_CHUNK', { pairKey, reportName, headers, rows: chunk.splice(0, chunk.length) });
        }
      };

      let i = 0;
      let j = 0;

      while (i < baseKeys.length || j < otherKeys.length) {
        if (abortSignal.aborted) break;
        const bk = i < baseKeys.length ? baseKeys[i] : null;
        const ok = j < otherKeys.length ? otherKeys[j] : null;

        if (bk != null && (ok == null || bk < ok)) {
          if (reportName === 'missing_in_other') {
            const bp = i;
            const bAmt = getAmountAt(basePack, bp);
            const keep = basePack.keepPreview.get(bk) || [];
            pushRow({
              txid: bk,
              mismatch_type: 'missing_in_other',
              base_amount_scaled: bAmt == null ? '' : bAmt.toString(),
              other_amount_scaled: '',
              base_status: (basePack.statusRaw[bp] || ''),
              other_status: '',
              ...Object.fromEntries((pr.keepCols || []).map((c, idx) => ['keep_' + c, keep[idx] ?? ''])),
            });
          }
          i++;
          continue;
        }

        if (ok != null && (bk == null || ok < bk)) {
          if (reportName === 'missing_in_base') {
            const op = j;
            const oAmt = getAmountAt(otherPack, op);
            const keep = otherPack.keepPreview.get(ok) || [];
            pushRow({
              txid: ok,
              mismatch_type: 'missing_in_base',
              base_amount_scaled: '',
              other_amount_scaled: oAmt == null ? '' : oAmt.toString(),
              base_status: '',
              other_status: (otherPack.statusRaw[op] || ''),
              ...Object.fromEntries((pr.keepCols || []).map((c, idx) => ['keep_' + c, keep[idx] ?? ''])),
            });
          }
          j++;
          continue;
        }

        // equal
        const k = bk;

        const bp = i;
        const op = j;

        const bAmtV = getAmountAt(basePack, bp);
        const oAmtV = getAmountAt(otherPack, op);
        const bAmt = bAmtV == null ? null : bAmtV.toString();
        const oAmt = oAmtV == null ? null : oAmtV.toString();

        const bStatCode = basePack.statusCode[bp];
        const oStatCode = otherPack.statusCode[op];

        const amountParseError = (bAmtV == null) !== (oAmtV == null);
        let amountMismatch = false;
        if (!amountParseError && bAmtV != null && oAmtV != null) {
          const bb = bAmtV;
          const oo = oAmtV;
          const diff = bb > oo ? (bb - oo) : (oo - bb);
          amountMismatch = diff > tol;
        }
        const statusMismatch = bStatCode !== oStatCode;

        if (amountParseError || amountMismatch || statusMismatch) {
          if (reportName === 'mismatches') {
            let mismatch_type = 'mismatch';
            if (amountParseError) mismatch_type = 'amount_missing_one_side';
            else if (amountMismatch && statusMismatch) mismatch_type = 'amount_and_status_mismatch';
            else if (amountMismatch) mismatch_type = 'amount_mismatch';
            else if (statusMismatch) mismatch_type = 'status_mismatch';

            const keep = basePack.keepPreview.get(k) || otherPack.keepPreview.get(k) || [];
            pushRow({
              txid: k,
              mismatch_type,
              base_amount_scaled: bAmt ?? '',
              other_amount_scaled: oAmt ?? '',
              base_status: (basePack.statusRaw[bp] || ''),
              other_status: (otherPack.statusRaw[op] || ''),
              ...Object.fromEntries((pr.keepCols || []).map((c, idx) => ['keep_' + c, keep[idx] ?? ''])),
            });
          }
        }

        i++;
        j++;
      }

      if (chunk.length) post('EXPORT_CHUNK', { pairKey, reportName, headers, rows: chunk });
      post('EXPORT_DONE', { pairKey, reportName, headers });
      return;
    }

  } catch (err) {
    post('ERROR', { message: (err && err.message) ? err.message : String(err) });
  }
};
