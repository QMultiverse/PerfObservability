// Fills in the contents page after a build, where that is possible.
//
// Word computes the page numbers, so this needs Microsoft Word, driven through
// tools/update_toc.ps1, which only exists on Windows. Elsewhere the build still
// succeeds and this says what to do by hand.
// Usage: node toc.js <path-to-docx>
const { spawnSync } = require("child_process");
const path = require("path");

const docx = path.resolve(process.argv[2]);
const manual =
  "Open the document, right-click the contents list and choose Update Field, then Update entire table.";

if (process.platform !== "win32") {
  console.log(`contents page not filled in automatically on ${process.platform}. ${manual}`);
  process.exit(0);
}
const script = path.join(__dirname, "..", "tools", "update_toc.ps1");
const run = spawnSync(
  "powershell",
  ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script, docx],
  { stdio: "inherit" }
);
if (run.error || run.status !== 0) {
  console.log(`could not update the contents page automatically. ${manual}`);
}
