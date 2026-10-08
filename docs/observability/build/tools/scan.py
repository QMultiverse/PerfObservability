"""Check a rendered PDF: page count, missing images, where chapters start: scan.py <pdf>."""

import os
import sys

# Optional: a folder where the tool's Python packages were installed with pip --target.
if os.environ.get("DOC_TOOLS_DEPS"):
    sys.path.insert(0, os.environ["DOC_TOOLS_DEPS"])
import pymupdf

CHAPTERS = (
    "5. Dashboards",
    "8. Scenario 1",
    "9. Scenario 2",
    "10. Scenario 3",
    "11. Scenario 4",
    "12. Scenario 5",
    "13. Findings",
)

doc = pymupdf.open(sys.argv[1])
print("pages:", doc.page_count)
missing = [i + 1 for i, page in enumerate(doc) if "missing image" in page.get_text()]
print("pages with missing images:", missing or "none")
for i, page in enumerate(doc):
    text = page.get_text()
    for chapter in CHAPTERS:
        if text.lstrip().startswith(chapter) or ("\n" + chapter) in text[:400]:
            print(f"  p{i + 1}: {chapter}")
