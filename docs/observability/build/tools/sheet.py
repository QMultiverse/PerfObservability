"""Contact sheet of rendered pages: sheet.py <dir> <pages> <out.png> [columns]."""

import os
import sys

# Optional: a folder where the tool's Python packages were installed with pip --target.
if os.environ.get("DOC_TOOLS_DEPS"):
    sys.path.insert(0, os.environ["DOC_TOOLS_DEPS"])
from PIL import Image

GAP = 10
pages = [Image.open(f"{sys.argv[1]}/page-{int(n):03d}.png") for n in sys.argv[2].split(",")]
width, height = pages[0].size
cols = int(sys.argv[4]) if len(sys.argv) > 4 else 3
rows = (len(pages) + cols - 1) // cols
size = (cols * width + (cols + 1) * GAP, rows * height + (rows + 1) * GAP)
sheet = Image.new("RGB", size, (120, 120, 120))
for i, page in enumerate(pages):
    sheet.paste(page, (GAP + (i % cols) * (width + GAP), GAP + (i // cols) * (height + GAP)))
sheet.save(sys.argv[3])
