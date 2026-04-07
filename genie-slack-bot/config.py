"""Configuration — reads from env vars, validates required values."""

import os
import sys

REQUIRED_VARS = [
    "SLACK_BOT_TOKEN",
    "SLACK_APP_TOKEN",
    "DATABRICKS_GENIE_SPACE_ID",
]


def validate_config() -> dict[str, str]:
    config = {}
    missing = []
    for var in REQUIRED_VARS:
        val = os.environ.get(var, "")
        if not val:
            # Also check without DATABRICKS_ prefix for GENIE_SPACE_ID
            if var == "DATABRICKS_GENIE_SPACE_ID":
                val = os.environ.get("GENIE_SPACE_ID", "")
            if not val:
                missing.append(var)
        config[var] = val
    if missing:
        print(f"ERROR: Missing required env vars: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return config
