"""Render pages of a PDF to PNG: topng.py <pdf> <outdir> [comma-separated page indexes]."""

import os
import sys

# Optional: a folder where the tool's Python packages were installed with pip --target.
if os.environ.get("DOC_TOOLS_DEPS"):
    sys.path.insert(0, os.environ["DOC_TOOLS_DEPS"])
import pymupdf

doc = pymupdf.open(sys.argv[1])
print("pages:", doc.page_count)
pages = [int(x) for x in sys.argv[3].split(",")] if len(sys.argv) > 3 else range(doc.page_count)
for i in pages:
    doc[i].get_pixmap(dpi=70).save(f"{sys.argv[2]}/page-{i + 1:03d}.png")
