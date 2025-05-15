import json
import os
import time
import logging
from datetime import datetime 
from typing import List, Dict, Any, Optional

def logger(key: str, namespace: str, command: str, value: Any) -> None:
    log_entry = {
        "key": key,
        "namespace": namespace,
        "command": command,
        "value": value,
        "timestamp": datetime.now().isoformat()
    }
    log_file = "logs.json"
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            logs = json.load(f)
    else:
        logs = []
    logs.append(log_entry)
    with open(log_file, "w") as f:
        json.dump(logs, f, indent=4)