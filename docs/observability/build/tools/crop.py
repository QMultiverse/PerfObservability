"""Trim the empty page background below and beside each Grafana panel screenshot."""

import os
import pathlib
import sys

# Optional: a folder where the tool's Python packages were installed with pip --target.
if os.environ.get("DOC_TOOLS_DEPS"):
    sys.path.insert(0, os.environ["DOC_TOOLS_DEPS"])

from PIL import Image, ImageChops

SRC = pathlib.Path(sys.argv[1])
DST = pathlib.Path(sys.argv[2])
DST.mkdir(parents=True, exist_ok=True)

for f in sorted(SRC.glob("*.png")):
    img = Image.open(f).convert("RGB")
    if f.name.startswith(("s", "ref_")) and not f.name.startswith("shot"):
        # Grafana's page background is the bottom-right pixel; the panel is white.
        bg = Image.new("RGB", img.size, img.getpixel((img.width - 1, img.height - 1)))
        box = ImageChops.difference(img, bg).getbbox()
        if box:
            left, top, right, bottom = box
            img = img.crop((0, 0, min(img.width, right + 6), min(img.height, bottom + 6)))
    img.save(DST / f.name, optimize=True)
    print(f.name, img.size)
