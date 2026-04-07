"""MAS (Multi-Agent Supervisor) client — calls a Databricks serving endpoint."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient
from openai import OpenAI

logger = logging.getLogger(__name__)


@dataclass
class MASResult:
    status: str  # COMPLETED, FAILED
    content: str = ""
    conversation_id: str = ""
    error: str = ""


class MASClient:
    def __init__(self, endpoint_name: str):
        self.endpoint_name = endpoint_name

        if os.environ.get("DATABRICKS_APP_NAME"):
            self.w = WorkspaceClient()
        else:
            profile = os.environ.get("DATABRICKS_PROFILE", "fe-vm-otto-demo")
            self.w = WorkspaceClient(profile=profile)

        # Build OpenAI client pointing at Databricks serving endpoint
        host = self.w.config.host.rstrip("/")
        token = self.w.config.authenticate()
        self.client = OpenAI(
            base_url=f"{host}/serving-endpoints",
            api_key=token,
        )

    def ask_question(self, question: str, history: list[dict] | None = None) -> MASResult:
        """Send a question to the MAS endpoint."""
        messages = history or []
        messages.append({"role": "user", "content": question})

        try:
            response = self.client.chat.completions.create(
                model=self.endpoint_name,
                messages=messages,
                temperature=0.0,
            )

            content = response.choices[0].message.content or ""
            return MASResult(
                status="COMPLETED",
                content=content,
                conversation_id=response.id,
            )
        except Exception as e:
            logger.exception("MAS endpoint call failed")
            return MASResult(
                status="FAILED",
                error=str(e),
            )
