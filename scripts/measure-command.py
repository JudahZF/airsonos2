#!/usr/bin/env python3
"""Measure one command's elapsed time, process CPU and peak RSS (Unix)."""
import json
import resource
import subprocess
import sys
import time

started = time.monotonic()
result = subprocess.run(sys.argv[1:], check=False)
usage = resource.getrusage(resource.RUSAGE_CHILDREN)
print(json.dumps({"elapsed_seconds": round(time.monotonic() - started, 6), "cpu_seconds": round(usage.ru_utime + usage.ru_stime, 6), "peak_rss_kib": usage.ru_maxrss // 1024 if sys.platform == "darwin" else usage.ru_maxrss, "exit_code": result.returncode}))
sys.exit(result.returncode)
