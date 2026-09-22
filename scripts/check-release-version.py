#!/usr/bin/env python3
"""Fail before publication unless the tag, Cargo and HA versions name this commit."""
import re
import subprocess
import sys
import tomllib
from pathlib import Path

raw = sys.argv[1] if len(sys.argv) == 2 else ""
version = raw.removeprefix("v")
if not re.fullmatch(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", version):
    sys.exit("Release version must be a semantic version, with an optional v prefix")
cargo = tomllib.loads(Path("Cargo.toml").read_text())["workspace"]["package"]["version"]
match = re.search(r'^version: "([^"]+)"$', Path("airsonos2/config.yaml").read_text(), re.M)
if not match or version != match[1] or version != cargo:
    sys.exit(f"Version mismatch: requested={version}, Cargo={cargo}, HA={match[1] if match else 'missing'}")
head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
for tag in dict.fromkeys([raw, f"v{version}", version]):
    result = subprocess.run(["git", "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}"], capture_output=True, text=True)
    if result.returncode == 0 and result.stdout.strip() == head:
        print(version)
        break
else:
    sys.exit("Release tag must exist and point to the checked-out commit; no image was published")
