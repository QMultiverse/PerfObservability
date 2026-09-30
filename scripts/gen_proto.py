r"""Regenerate the gRPC / protobuf stubs from proto/ into gen/hub_proto/.

Generated code is never hand-edited and never committed (gen/ is gitignored).

    python scripts/gen_proto.py          # PowerShell:  .\.venv\Scripts\python scripts\gen_proto.py
    python scripts/gen_proto.py --check  # fail if the stubs are missing or stale

grpcio-tools emits absolute imports rooted at the proto path (``hub.v1.x_pb2``),
which would collide with the ``hub`` service package. We generate into a
``hub_proto`` namespace package and rewrite those imports to match.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT / "proto"
OUT_DIR = ROOT / "gen" / "hub_proto"
PACKAGES = ("hub", "ext", "ess")

# ``import hub.v1.common_pb2`` / ``from hub.v1 import common_pb2`` -> hub_proto.*
_IMPORT_RE = re.compile(
    r"^(?P<indent>\s*)(?:from (?P<from>(?:hub|ext|ess)(?:\.\w+)*) import |"
    r"import (?P<import>(?:hub|ext|ess)(?:\.\w+)*))",
    re.MULTILINE,
)


def proto_files() -> list[Path]:
    return sorted(PROTO_DIR.rglob("*.proto"))


def generate() -> None:
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True)

    files = [str(p.relative_to(PROTO_DIR).as_posix()) for p in proto_files()]
    if not files:
        raise SystemExit(f"no .proto files under {PROTO_DIR}")

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"--proto_path={PROTO_DIR}",
        f"--python_out={OUT_DIR}",
        f"--pyi_out={OUT_DIR}",
        f"--grpc_python_out={OUT_DIR}",
        *files,
    ]
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        sys.stderr.write(result.stdout + result.stderr)
        raise SystemExit(f"protoc failed with exit code {result.returncode}")

    _add_init_files()
    _rewrite_imports()
    print(f"generated {len(files)} proto file(s) into {OUT_DIR.relative_to(ROOT)}")


def _add_init_files() -> None:
    (OUT_DIR / "__init__.py").write_text(
        '"""Generated protobuf and gRPC stubs. Do not edit; see scripts/gen_proto.py."""\n',
        encoding="utf-8",
    )
    for path in OUT_DIR.rglob("*"):
        if path.is_dir():
            (path / "__init__.py").touch()


def _rewrite_imports() -> None:
    def repl(match: re.Match[str]) -> str:
        indent = match.group("indent")
        if match.group("from"):
            return f"{indent}from hub_proto.{match.group('from')} import "
        return f"{indent}import hub_proto.{match.group('import')}"

    for path in OUT_DIR.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        new = _IMPORT_RE.sub(repl, text)
        # grpc stubs also reference the module by its dotted name in the body.
        for pkg in PACKAGES:
            new = re.sub(rf"(?<![\w.]){pkg}\.v1\.(\w+_pb2)\b", rf"hub_proto.{pkg}.v1.\1", new)
        if new != text:
            path.write_text(new, encoding="utf-8")

    for path in OUT_DIR.rglob("*.pyi"):
        text = path.read_text(encoding="utf-8")
        new = _IMPORT_RE.sub(repl, text)
        if new != text:
            path.write_text(new, encoding="utf-8")


def check() -> int:
    if not OUT_DIR.exists():
        print("stubs missing: run python scripts/gen_proto.py", file=sys.stderr)
        return 1
    newest_proto = max(p.stat().st_mtime for p in proto_files())
    generated = list(OUT_DIR.rglob("*_pb2.py"))
    if not generated:
        print("stubs missing: run python scripts/gen_proto.py", file=sys.stderr)
        return 1
    oldest_stub = min(p.stat().st_mtime for p in generated)
    if newest_proto > oldest_stub:
        print("stubs are stale: run python scripts/gen_proto.py", file=sys.stderr)
        return 1
    print("stubs are up to date")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="verify without regenerating")
    args = parser.parse_args()
    if args.check:
        return check()
    generate()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
