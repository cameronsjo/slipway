#!/usr/bin/env python3
"""Set one image-lock.json binary hash field by hashing the real binary.

usage: relock-binary.py <image-lock.json> <field> <binary-path>
"""

import hashlib
import json
import sys
from pathlib import Path

lock_path, field, binary = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
lock = json.loads(lock_path.read_text())
if field not in lock:
    raise SystemExit(f"{lock_path}: no field {field}")
lock[field] = hashlib.sha256(binary.read_bytes()).hexdigest()
lock_path.write_text(json.dumps(lock, indent=2) + "\n")
print(f"{lock_path.name}: {field} = {lock[field]}")
