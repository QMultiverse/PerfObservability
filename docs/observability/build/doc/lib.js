// Building blocks for the observability guide (docx-js).
const fs = require("fs");
const path = require("path");
const d = require("docx");

const FONT = "Calibri";
const MONO = "Consolas";
const ACCENT = "1F4E8C";
const MUTED = "5A6270";
const PAGE_W = 11906; // A4, DXA
const MARGIN = 1134;  // 2 cm
const TEXT_W = PAGE_W - 2 * MARGIN; // 9638 DXA = 6.69 in
const IMG_W = 640;    // px at 96 dpi, fits the text width

let figureNo = 0;
let tableNo = 0;
let listNo = 0;

// **bold**, `code` and _italic_ inside one string.
function runs(text, base = {}) {
  const out = [];
  const re = /(\*\*[^*]+\*\*|`[^`]+`|_[^_]+_)/g;
  let last = 0;
  let m;
  while ((m = re.exec(text)) !== null) {
    if (m.index > last) out.push(new d.TextRun({ text: text.slice(last, m.index), ...base }));
    const t = m[0];
    if (t.startsWith("**")) out.push(new d.TextRun({ text: t.slice(2, -2), bold: true, ...base }));
    else if (t.startsWith("`")) out.push(new d.TextRun({ text: t.slice(1, -1), font: MONO, size: 19, color: "8A2D3B", ...base }));
    else out.push(new d.TextRun({ text: t.slice(1, -1), italics: true, ...base }));
    last = m.index + t.length;
  }
  if (last < text.length) out.push(new d.TextRun({ text: text.slice(last), ...base }));
  return out;
}

const p = (text, opts = {}) =>
  new d.Paragraph({ children: runs(text), spacing: { after: 120, line: 288 }, ...opts });

const h1 = (text, pageBreak = true) =>
  new d.Paragraph({ heading: d.HeadingLevel.HEADING_1, pageBreakBefore: pageBreak, children: [new d.TextRun(text)] });
const h2 = (text) => new d.Paragraph({ heading: d.HeadingLevel.HEADING_2, children: [new d.TextRun(text)] });
const h3 = (text) => new d.Paragraph({ heading: d.HeadingLevel.HEADING_3, children: [new d.TextRun(text)] });

function bullets(items, level = 0) {
  return items.map((it) =>
    Array.isArray(it)
      ? bullets(it, level + 1)
      : new d.Paragraph({ numbering: { reference: "bullets", level }, children: runs(it), spacing: { after: 60, line: 276 } })
  ).flat();
}

function numbered(items) {
  listNo += 1;
  const ref = `num${listNo}`;
  return items.map((it) =>
    new d.Paragraph({ numbering: { reference: ref, level: 0 }, children: runs(it), spacing: { after: 60, line: 276 } })
  );
}

function code(lines) {
  const arr = Array.isArray(lines) ? lines : lines.split("\n");
  return arr.map((line, i) =>
    new d.Paragraph({
      children: [new d.TextRun({ text: line.length ? line : " ", font: MONO, size: 18, color: "1E2430" })],
      shading: { type: d.ShadingType.CLEAR, fill: "F1F3F6", color: "auto" },
      spacing: { before: i === 0 ? 60 : 0, after: i === arr.length - 1 ? 140 : 0, line: 240 },
      indent: { left: 120, right: 120 },
      border: i === 0 ? { top: { style: d.BorderStyle.SINGLE, size: 4, color: "D5D9E0", space: 4 } } :
              i === arr.length - 1 ? { bottom: { style: d.BorderStyle.SINGLE, size: 4, color: "D5D9E0", space: 4 } } : undefined,
    })
  );
}

const CALLOUTS = {
  key: { label: "Key idea", fill: "EAF1FB", line: "2F6DB5" },
  lesson: { label: "Lesson", fill: "EAF6EE", line: "2E8B57" },
  warn: { label: "Watch out", fill: "FFF4E5", line: "C9761A" },
  wrong: { label: "What went wrong along the way", fill: "FBEDEE", line: "B03A48" },
};

function callout(kind, paragraphs) {
  const c = CALLOUTS[kind];
  const paras = Array.isArray(paragraphs) ? paragraphs : [paragraphs];
  const border = { left: { style: d.BorderStyle.SINGLE, size: 24, color: c.line, space: 8 } };
  const shade = { type: d.ShadingType.CLEAR, fill: c.fill, color: "auto" };
  const out = [new d.Paragraph({
    children: [new d.TextRun({ text: c.label, bold: true, color: c.line })],
    shading: shade, border, spacing: { before: 120, after: 40 }, indent: { left: 200, right: 120 },
  })];
  paras.forEach((t, i) => out.push(new d.Paragraph({
    children: runs(t), shading: shade, border,
    spacing: { after: i === paras.length - 1 ? 160 : 60, line: 276 }, indent: { left: 200, right: 120 },
  })));
  return out;
}

function pngSize(file) {
  const b = fs.readFileSync(file);
  return { w: b.readUInt32BE(16), h: b.readUInt32BE(20) };
}

function figure(file, caption, widthPx = IMG_W) {
  if (!fs.existsSync(file)) {
    return [p(`[missing image: ${path.basename(file)}]`)];
  }
  figureNo += 1;
  const { w, h } = pngSize(file);
  const width = Math.min(widthPx, IMG_W);
  const height = Math.round((h / w) * width);
  return [
    new d.Paragraph({
      alignment: d.AlignmentType.CENTER, keepNext: true, spacing: { before: 120, after: 60 },
      children: [new d.ImageRun({ type: "png", data: fs.readFileSync(file), transformation: { width, height },
        altText: { title: `Figure ${figureNo}`, description: caption, name: path.basename(file) } })],
    }),
    new d.Paragraph({
      alignment: d.AlignmentType.LEFT, spacing: { after: 200, line: 264 },
      children: [new d.TextRun({ text: `Figure ${figureNo}. `, bold: true, size: 19, color: ACCENT }), ...runs(caption, { size: 19, color: MUTED })],
    }),
  ];
}

function table(headers, rows, widths, opts = {}) {
  const total = widths.reduce((a, b) => a + b, 0);
  const scale = TEXT_W / total;
  const cols = widths.map((w) => Math.floor(w * scale));
  cols[cols.length - 1] += TEXT_W - cols.reduce((a, b) => a + b, 0);
  const border = { style: d.BorderStyle.SINGLE, size: 4, color: "C8CDD5" };
  const borders = { top: border, bottom: border, left: border, right: border };
  const cell = (text, i, head) => new d.TableCell({
    width: { size: cols[i], type: d.WidthType.DXA },
    borders,
    shading: head ? { type: d.ShadingType.CLEAR, fill: "E3EAF4", color: "auto" } : undefined,
    margins: { top: 60, bottom: 60, left: 100, right: 100 },
    children: String(text).split("\n").map((line) => new d.Paragraph({
      children: runs(line, head ? { bold: true, size: 19 } : { size: 19 }),
      spacing: { after: 20, line: 252 },
    })),
  });
  const rowsOut = [
    ...(headers ? [new d.TableRow({ tableHeader: true, cantSplit: true, children: headers.map((h, i) => cell(h, i, true)) })] : []),
    ...rows.map((r) => new d.TableRow({ cantSplit: true, children: r.map((c, i) => cell(c, i, false)) })),
  ];
  const out = [new d.Table({ width: { size: TEXT_W, type: d.WidthType.DXA }, columnWidths: cols, rows: rowsOut })];
  if (opts.caption) {
    tableNo += 1;
    out.push(new d.Paragraph({ spacing: { before: 60, after: 200 },
      children: [new d.TextRun({ text: `Table ${tableNo}. `, bold: true, size: 19, color: ACCENT }), ...runs(opts.caption, { size: 19, color: MUTED })] }));
  } else {
    out.push(new d.Paragraph({ spacing: { after: 120 }, children: [] }));
  }
  return out;
}

function numberingConfig() {
  const bulletLevels = [0, 1, 2].map((level) => ({
    level, format: d.LevelFormat.BULLET, text: ["•", "–", "◦"][level], alignment: d.AlignmentType.LEFT,
    style: { paragraph: { indent: { left: 360 * (level + 1), hanging: 260 } } },
  }));
  const configs = [{ reference: "bullets", levels: bulletLevels }];
  for (let i = 1; i <= 120; i++) {
    configs.push({ reference: `num${i}`, levels: [{ level: 0, format: d.LevelFormat.DECIMAL, text: "%1.", alignment: d.AlignmentType.LEFT,
      style: { paragraph: { indent: { left: 360, hanging: 300 } } } }] });
  }
  return configs;
}

module.exports = { d, p, h1, h2, h3, bullets, numbered, code, callout, figure, table, numberingConfig, runs,
  FONT, MONO, ACCENT, MUTED, PAGE_W, MARGIN, TEXT_W };
