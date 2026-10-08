# Building the observability guide

`../Payment-Hub-Observability-Guide.docx` is generated from the files in this
folder. Edit the content here and rebuild; don't edit the `.docx` by hand, or
the next build will overwrite your changes.

```
build/
├── doc/            the generator (Node, docx-js)
│   ├── build.js    title page, contents, styles, headers and footers
│   ├── lib.js      building blocks: headings, tables, figures, callouts, code
│   ├── part1.js    1 Start here · 2 Concepts · 3 Setup · 4 Stack tour
│   ├── part2.js    5 Dashboards and every panel
│   ├── part3.js    6 Running a test · 7 Triage method
│   ├── part4.js    8-12 The five scenarios
│   └── part5.js    13 Findings · 14 Glossary · Appendices
├── assets/
│   ├── shots/      Grafana screenshots used in the guide (81)
│   └── diagrams/   stack and triage diagrams
└── tools/          optional: capture, crop, draw and check (Python)
```

## Rebuild the document

Needs Node.js 18 or later. The same on Windows, macOS and Linux:

```sh
cd docs/observability/build/doc
npm install          # once; installs docx-js
npm run build        # writes ../../Payment-Hub-Observability-Guide.docx
```

`npm run build` has two steps:

1. `build.js` writes the `.docx`.
2. `toc.js` fills in the contents page. On Windows it runs
   `tools/update_toc.ps1`, which opens the document in a hidden Microsoft
   Word, lays out the pages and fills in the list with Word's own page numbers.

The second step needs Word on Windows. On macOS and Linux, or without Word,
the build still succeeds and says so; open the document, right-click the
contents list and choose **Update Field**, then **Update entire table**.

Why not let Word refresh the list when the file opens? It did, and every
entry came out as page 1: the refresh ran before Word had laid out a long
document full of images. Close the document before rebuilding, because Word
locks a file while it is open.

The screenshots in `assets/shots/` are kept on purpose. Prometheus keeps
metrics for 24 hours, so the scenario runs they show can't be captured again;
scenario 1's data was already gone by the time the guide was finished.

## Optional tools (Python)

Only needed to capture new screenshots, redraw the diagrams or check the
rendered pages. They are not project dependencies.

| Tool | What it does | Needs |
| --- | --- | --- |
| `update_toc.ps1 <docx>` | Fills in the contents page numbers (run by `npm run build`) | Microsoft Word |
| `capture.py <outdir> <03-performance.json>` | Screenshots each scenario's panels and all 40 reference panels from Grafana, using a headless Chrome, Chromium or Edge, found automatically on each platform (set `DOC_BROWSER` to choose). Skips files that already exist. The scenario time windows are in the script | A Chromium-based browser, the stack running, the metrics still in Prometheus |
| `crop.py <src> <dst>` | Trims the empty background around each panel screenshot | Pillow |
| `diagrams.py <outdir>` | Draws `diagram_stack.png` and `diagram_triage.png` | PyMuPDF |
| `scan.py <pdf>` | Page count, missing images, where each chapter starts | PyMuPDF |
| `topng.py <pdf> <outdir> [pages]` | Renders PDF pages to PNG | PyMuPDF |
| `sheet.py <dir> <pages> <out.png> [cols]` | Puts several rendered pages side by side | Pillow |

**PyMuPDF is licensed under the AGPL.** `CLAUDE.md` asks that restrictively
licensed dependencies are not added to the project without agreement, so
install it only in a throwaway folder for local use, never in
`pyproject.toml`. To keep the tools' packages out of your environment:

```sh
python -m pip install --target docs/observability/build/.deps pillow pymupdf
export DOC_TOOLS_DEPS="$PWD/docs/observability/build/.deps"
python docs/observability/build/tools/crop.py <src> <dst>
```

In PowerShell, set the variable with
`$env:DOC_TOOLS_DEPS = "$PWD/docs/observability/build/.deps"` instead of `export`.

To render the `.docx` to PDF for checking, LibreOffice works:

```sh
soffice --headless --convert-to pdf --outdir docs/observability/build/render \
  docs/observability/Payment-Hub-Observability-Guide.docx
```

`soffice` is `C:\Program Files\LibreOffice\program\soffice.exe` on Windows and
`/Applications/LibreOffice.app/Contents/MacOS/soffice` on macOS.

## Capturing new screenshots

1. Run the scenario, with annotations, as described in chapter 6 of the guide.
2. Put its start and end times (milliseconds since 1970) in `SCENARIOS` in
   `tools/capture.py`, with the panel ids to capture (`03-performance.json`
   lists them).
3. Capture **within 24 hours**, then crop into `assets/shots/` and rebuild.
4. Stop the ELK profile first on a 16 GB machine
   (`docker compose stop elasticsearch kibana filebeat`): a full stack plus
   the headless browser ran the machine out of memory.
