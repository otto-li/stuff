"""Databricks Genie API client using the SDK."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import timedelta

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

TIMEOUT = timedelta(minutes=3)


@dataclass
class GenieResult:
    status: str  # COMPLETED, FAILED, etc.
    narrative: str = ""
    sql: str = ""
    columns: list[str] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)
    conversation_id: str = ""
    message_id: str = ""
    error: str = ""


class DatabricksGenieClient:
    def __init__(self, space_id: str):
        self.space_id = space_id
        if os.environ.get("DATABRICKS_APP_NAME"):
            self.w = WorkspaceClient()
        else:
            profile = os.environ.get("DATABRICKS_PROFILE", "fe-vm-otto-demo")
            self.w = WorkspaceClient(profile=profile)

    def ask_question(self, question: str, conversation_id: str | None = None) -> GenieResult:
        """Ask Genie a question. Starts or continues a conversation."""
        if conversation_id:
            return self._continue_conversation(conversation_id, question)
        return self._start_conversation(question)

    def send_feedback(self, conversation_id: str, message_id: str, rating: str) -> None:
        """Send feedback (POSITIVE/NEGATIVE) for a Genie message."""
        try:
            self.w.api_client.do(
                "POST",
                f"/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages/{message_id}/feedback",
                body={"rating": rating},
            )
            logger.info("Sent %s feedback for message %s", rating, message_id)
        except Exception as e:
            logger.warning("Failed to send feedback: %s", e)

    def _start_conversation(self, question: str) -> GenieResult:
        wait = self.w.genie.start_conversation(space_id=self.space_id, content=question)
        conversation_id = wait.response.conversation_id
        message_id = wait.response.message_id
        msg = wait.result(timeout=TIMEOUT)
        return self._parse_message(msg, conversation_id, message_id)

    def _continue_conversation(self, conversation_id: str, question: str) -> GenieResult:
        wait = self.w.genie.create_message(
            space_id=self.space_id, conversation_id=conversation_id, content=question
        )
        message_id = wait._bind.get("message_id", "")
        msg = wait.result(timeout=TIMEOUT)
        return self._parse_message(msg, conversation_id, message_id)

    def _parse_message(self, msg, conversation_id: str, message_id: str) -> GenieResult:
        result = GenieResult(
            status=msg.status.value if msg.status else "UNKNOWN",
            conversation_id=conversation_id,
            message_id=message_id,
        )

        if msg.error:
            result.error = str(msg.error)

        if not msg.attachments:
            return result

        for attachment in msg.attachments:
            if attachment.text and attachment.text.content:
                result.narrative = attachment.text.content

            if attachment.query and attachment.query.query:
                result.sql = attachment.query.query

            att_id = getattr(attachment, "attachment_id", None) or getattr(attachment, "id", None)
            if attachment.query and att_id:
                try:
                    qr = self.w.genie.get_message_attachment_query_result(
                        space_id=self.space_id,
                        conversation_id=conversation_id,
                        message_id=message_id,
                        attachment_id=att_id,
                    )
                    if qr.statement_response:
                        sr = qr.statement_response
                        if sr.manifest and sr.manifest.schema and sr.manifest.schema.columns:
                            result.columns = [c.name for c in sr.manifest.schema.columns]
                        if sr.result and sr.result.data_array:
                            result.rows = [
                                [str(cell) if cell is not None else "" for cell in row]
                                for row in sr.result.data_array
                            ]
                except Exception as e:
                    logger.debug("Could not fetch query result: %s", e)

            for sq in getattr(attachment, "suggested_questions", None) or []:
                q = getattr(sq, "question", None) or getattr(sq, "text", None)
                if q:
                    result.suggestions.append(q)

        return result
