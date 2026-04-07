"""Slack bot — handles @mentions, DMs, and feedback buttons via Socket Mode."""

from __future__ import annotations

import logging
import re

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from chart_generator import generate_chart
from databricks_genie_client import DatabricksGenieClient, GenieResult

logger = logging.getLogger(__name__)

MAX_TABLE_ROWS = 10
MAX_COL_WIDTH = 20


class SlackGenieBot:
    def __init__(self, bot_token: str, app_token: str, genie_client: DatabricksGenieClient):
        self.genie_client = genie_client
        self.app_token = app_token

        # slack_thread_ts -> genie_conversation_id
        self.conversation_map: dict[str, str] = {}
        # slack_message_ts -> (conversation_id, message_id) for feedback
        self.feedback_map: dict[str, tuple[str, str]] = {}

        self.app = App(token=bot_token)
        self._register_handlers()

    def start(self):
        handler = SocketModeHandler(self.app, self.app_token)
        handler.start()

    def _register_handlers(self):
        @self.app.event("app_mention")
        def handle_mention(event, say):
            question = re.sub(r"<@[A-Z0-9]+>", "", event.get("text", "")).strip()
            thread_ts = event.get("thread_ts") or event.get("ts", "")
            self._handle_question(question, thread_ts, say)

        @self.app.event("message")
        def handle_dm(event, say):
            # Only respond to DMs and threaded replies
            if event.get("channel_type") != "im" and "thread_ts" not in event:
                return
            # Ignore bot messages
            if event.get("bot_id") or event.get("subtype"):
                return
            question = event.get("text", "").strip()
            thread_ts = event.get("thread_ts") or event.get("ts", "")
            self._handle_question(question, thread_ts, say)

        @self.app.action("thumbs_up")
        def handle_thumbs_up(ack, body, client):
            ack()
            self._handle_feedback(body, client, "POSITIVE")

        @self.app.action("thumbs_down")
        def handle_thumbs_down(ack, body, client):
            ack()
            self._handle_feedback(body, client, "NEGATIVE")

    def _handle_question(self, question: str, thread_ts: str, say) -> None:
        if not question:
            say(text="Ask me a question! e.g. `@GenieBot what were sales last quarter?`", thread_ts=thread_ts)
            return

        thinking = say(text=":hourglass: Asking Genie...", thread_ts=thread_ts)
        thinking_ts = thinking["ts"]
        channel = thinking["channel"]

        try:
            conversation_id = self.conversation_map.get(thread_ts)
            result = self.genie_client.ask_question(question, conversation_id)

            if result.conversation_id:
                self.conversation_map[thread_ts] = result.conversation_id

            blocks = self._format_result_blocks(result)
            text = result.narrative or "Query complete"

            self.app.client.chat_update(
                channel=channel, ts=thinking_ts, text=text, blocks=blocks,
            )

            # Upload chart if data is chartable
            if result.columns and result.rows:
                try:
                    chart_png = generate_chart(result.columns, result.rows)
                    if chart_png:
                        self.app.client.files_upload_v2(
                            channel=channel,
                            thread_ts=thread_ts,
                            content=chart_png,
                            filename="chart.png",
                            title="Query Results Chart",
                        )
                except Exception as e:
                    logger.debug("Chart generation skipped: %s", e)

            # Store for feedback
            if result.conversation_id and result.message_id:
                self.feedback_map[thinking_ts] = (result.conversation_id, result.message_id)

        except Exception as e:
            logger.exception("Error handling Genie question")
            self.app.client.chat_update(
                channel=channel,
                ts=thinking_ts,
                text=f":warning: Error: {e}",
                blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": f":warning: *Error:* {e}"}}],
            )

    def _handle_feedback(self, body: dict, client, rating: str) -> None:
        message_ts = body.get("message", {}).get("ts", "")
        info = self.feedback_map.get(message_ts)
        if not info:
            logger.warning("No feedback mapping for message %s", message_ts)
            return

        conversation_id, message_id = info
        self.genie_client.send_feedback(conversation_id, message_id, rating)

        emoji = ":+1:" if rating == "POSITIVE" else ":-1:"
        user = body.get("user", {}).get("id", "someone")

        # Update the actions block to show confirmation
        original_blocks = body.get("message", {}).get("blocks", [])
        updated_blocks = [b for b in original_blocks if b.get("type") != "actions"]
        updated_blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"{emoji} Feedback recorded by <@{user}>"}],
        })

        client.chat_update(
            channel=body["channel"]["id"],
            ts=message_ts,
            text=body.get("message", {}).get("text", ""),
            blocks=updated_blocks,
        )

    def _format_result_blocks(self, result: GenieResult) -> list[dict]:
        blocks: list[dict] = []

        # Narrative
        if result.narrative:
            emoji = ":white_check_mark:" if result.status == "COMPLETED" else ":x:"
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"{emoji} {result.narrative}"},
            })

        # SQL
        if result.sql:
            sql_text = f"```{result.sql}```"
            if len(sql_text) > 2900:
                sql_text = f"```{result.sql[:2850]}...```"
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": sql_text}})

        # Table
        if result.columns and result.rows:
            table = self._format_table(result.columns, result.rows)
            if table:
                table_text = f"```\n{table}\n```"
                if len(table_text) > 2900:
                    table_text = f"```\n{table[:2850]}\n...```"
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": table_text}})

        # Suggested follow-ups
        if result.suggestions:
            suggestions = " | ".join(f"`{s}`" for s in result.suggestions[:3])
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f":bulb: Try: {suggestions}"}],
            })

        # Feedback buttons
        blocks.append({
            "type": "actions",
            "elements": [
                {"type": "button", "text": {"type": "plain_text", "text": ":+1:", "emoji": True}, "action_id": "thumbs_up"},
                {"type": "button", "text": {"type": "plain_text", "text": ":-1:", "emoji": True}, "action_id": "thumbs_down"},
            ],
        })

        # Error fallback
        if result.status == "FAILED" and not result.narrative:
            error_msg = result.error or "Genie could not answer this question. Try rephrasing."
            blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": f":x: {error_msg}"}}]

        return blocks

    @staticmethod
    def _format_table(columns: list[str], rows: list[list[str]]) -> str:
        display_rows = rows[:MAX_TABLE_ROWS]
        headers = [h[:MAX_COL_WIDTH] for h in columns]
        body = [[cell[:MAX_COL_WIDTH] for cell in row] for row in display_rows]

        widths = [len(h) for h in headers]
        for row in body:
            for i, cell in enumerate(row):
                if i < len(widths):
                    widths[i] = max(widths[i], len(cell))

        def fmt(cells):
            return "│ " + " │ ".join(
                c.rjust(widths[i]) if c.replace(".", "").replace("-", "").isdigit() else c.ljust(widths[i])
                for i, c in enumerate(cells) if i < len(widths)
            ) + " │"

        sep = "┼".join("─" * (w + 2) for w in widths)
        lines = [fmt(headers), sep]
        for row in body:
            lines.append(fmt(row))

        if len(rows) > MAX_TABLE_ROWS:
            lines.append(f"\nShowing {MAX_TABLE_ROWS} of {len(rows)} rows")

        return "\n".join(lines)
