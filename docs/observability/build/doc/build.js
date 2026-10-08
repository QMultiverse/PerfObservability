// Builds Payment-Hub-Observability-Guide.docx.
// Usage: node build.js <shots dir> <diagram dir> <output .docx>
const fs = require("fs");
const lib = require("./lib");
const { d, FONT, ACCENT, MUTED, PAGE_W, MARGIN } = lib;

const [SHOTS, DIAG, OUT] = process.argv.slice(2);

const titlePage = [
  new d.Paragraph({ spacing: { before: 2600 }, children: [] }),
  new d.Paragraph({ children: [new d.TextRun({ text: "Payment Hub", size: 34, color: MUTED, font: FONT })] }),
  new d.Paragraph({ spacing: { after: 200 }, children: [new d.TextRun({ text: "Observability and Performance Testing Guide", size: 56, bold: true, color: ACCENT, font: FONT })] }),
  new d.Paragraph({
    spacing: { after: 600 },
    border: { bottom: { style: d.BorderStyle.SINGLE, size: 12, color: ACCENT, space: 8 } },
    children: [new d.TextRun({ text: "Setting up the stack, reading the metrics, and five fault-injection scenarios worked through panel by panel", size: 26, color: MUTED, font: FONT })],
  }),
  ...[
    ["Author", "Jatin Mehta"],
    ["Version", "1.1"],
    ["Date", "8 October 2026"],
    ["Describes", "Branch main as of 8 October 2026"],
    ["Audience", "Engineers new to observability"],
  ].map(([k, v]) => new d.Paragraph({ spacing: { after: 80 }, children: [
    new d.TextRun({ text: `${k}:  `, bold: true, size: 22, font: FONT }),
    new d.TextRun({ text: v, size: 22, font: FONT }),
  ] })),
  new d.Paragraph({ spacing: { before: 1800 }, children: [new d.TextRun({ text: "All payment data shown is synthetic. Screenshots are from the local Docker stack on Windows.", italics: true, size: 18, color: MUTED, font: FONT })] }),
];

const tocPage = [
  // Styled like a heading but not one, so the contents list does not include itself.
  new d.Paragraph({ pageBreakBefore: true, spacing: { before: 120, after: 240 },
    children: [new d.TextRun({ text: "Contents", size: 36, bold: true, color: ACCENT, font: FONT })] }),
  new d.TableOfContents("Contents", { hyperlink: true, headingStyleRange: "1-2" }),
  new d.Paragraph({ spacing: { before: 200 }, children: [new d.TextRun({ text: "If the page numbers look wrong after editing, right-click the list and choose Update Field, then Update entire table.", italics: true, size: 18, color: MUTED })] }),
];

const body = [
  ...require("./part1")(DIAG, SHOTS),
  ...require("./part2")(SHOTS),
  ...require("./part3")(DIAG),
  ...require("./part4")(SHOTS),
  ...require("./part5")(),
].flat(Infinity).filter(Boolean);

const doc = new d.Document({
  creator: "Jatin Mehta",
  title: "Payment Hub: Observability and Performance Testing Guide",
  description: "Setup, stack tour, metrics reference and five fault-injection scenarios",
  // No updateFields: Word refreshes on open before it has laid the pages out,
  // and every entry came out as page 1. tools/update_toc.ps1 fills the list
  // in with Word's own page numbers after the build instead.
  numbering: { config: lib.numberingConfig() },
  styles: {
    default: { document: { run: { font: FONT, size: 21 }, paragraph: { spacing: { line: 288 } } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 36, bold: true, color: ACCENT, font: FONT },
        paragraph: { spacing: { before: 120, after: 240 }, outlineLevel: 0, keepNext: true } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 28, bold: true, color: "1E2430", font: FONT },
        paragraph: { spacing: { before: 320, after: 120 }, outlineLevel: 1, keepNext: true } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 23, bold: true, color: ACCENT, font: FONT },
        paragraph: { spacing: { before: 220, after: 80 }, outlineLevel: 2, keepNext: true } },
    ],
  },
  sections: [
    {
      properties: { page: { size: { width: PAGE_W, height: 16838 }, margin: { top: MARGIN, bottom: MARGIN, left: MARGIN, right: MARGIN } } },
      children: [...titlePage, ...tocPage],
    },
    {
      properties: { page: { size: { width: PAGE_W, height: 16838 }, margin: { top: MARGIN, bottom: MARGIN, left: MARGIN, right: MARGIN }, pageNumbers: { start: 1 } } },
      headers: { default: new d.Header({ children: [new d.Paragraph({ alignment: d.AlignmentType.RIGHT,
        children: [new d.TextRun({ text: "Payment Hub · Observability and Performance Testing Guide", size: 16, color: MUTED })] })] }) },
      footers: { default: new d.Footer({ children: [new d.Paragraph({ alignment: d.AlignmentType.CENTER,
        children: [new d.TextRun({ children: ["Page ", d.PageNumber.CURRENT, " of ", d.PageNumber.TOTAL_PAGES_IN_SECTION], size: 16, color: MUTED })] })] }) },
      children: body,
    },
  ],
});

d.Packer.toBuffer(doc).then((buf) => {
  fs.writeFileSync(OUT, buf);
  console.log(`wrote ${OUT} (${(buf.length / 1024 / 1024).toFixed(1)} MB), ${body.length} blocks`);
});
