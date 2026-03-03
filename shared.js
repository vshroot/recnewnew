/* shared.js — functions used by both app.js (main thread) and worker.js */

function pow10BigInt(n) {
  let x = 1n;
  for (let i = 0; i < n; i++) x *= 10n;
  return x;
}

function parseAmountScaled(raw, scale, decimalComma) {
  let s = (raw || "").trim();
  if (!s) return null;

  let neg = false;
  if (s.startsWith("(") && s.endsWith(")")) {
    neg = true;
    s = s.slice(1, -1).trim();
  }

  s = s.replaceAll("\u00a0", "").replaceAll(" ", "");

  if (decimalComma) {
    if (s.includes(",")) {
      s = s.replaceAll(".", "").replaceAll(",", ".");
    }
  } else {
    if (s.includes(",") && s.includes(".")) {
      s = s.replaceAll(",", "");
    } else if (s.includes(",") && !s.includes(".")) {
      s = s.replaceAll(",", ".");
    }
  }

  s = s.replace(/[^\d.+-]/g, "");
  if (!s || s === "-" || s === "+" || s === "." || s === "-." || s === "+.") return null;

  let sign = 1n;
  if (s.startsWith("-")) {
    sign = -1n;
    s = s.slice(1);
  } else if (s.startsWith("+")) {
    s = s.slice(1);
  }
  if (!s) return null;

  const parts = s.split(".");
  if (parts.length > 2) return null;
  let intPart  = parts[0] || "0";
  let fracPart = parts[1] || "";

  intPart = intPart.replace(/^0+(?=\d)/, "");
  if (!/^\d+$/.test(intPart)) return null;
  if (fracPart && !/^\d+$/.test(fracPart)) return null;

  const scaleN = Number(scale);
  if (!Number.isInteger(scaleN) || scaleN < 0 || scaleN > 18) return null;

  const factor = pow10BigInt(scaleN);
  let scaledAbs = BigInt(intPart) * factor;

  if (scaleN === 0) {
    if (fracPart && fracPart[0] >= "5") scaledAbs += 1n;
  } else {
    if (fracPart.length <= scaleN) {
      const fracPadded = (fracPart + "0".repeat(scaleN)).slice(0, scaleN);
      if (fracPadded) scaledAbs += BigInt(fracPadded);
    } else {
      const main      = fracPart.slice(0, scaleN);
      const nextDigit = fracPart[scaleN] || "0";
      if (main) scaledAbs += BigInt(main);
      if (nextDigit >= "5") scaledAbs += 1n;
    }
  }

  if (neg) sign = -sign;
  return sign * scaledAbs;
}

function normalizeStatus(raw, statusMap) {
  const s = (raw || "").trim();
  if (!s) return null;
  const lower = s.toLowerCase();
  return (statusMap && statusMap.get(lower)) || lower;
}
