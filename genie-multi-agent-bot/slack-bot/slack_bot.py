"""Slack bot — handles @mentions, DMs, and feedback buttons via Socket Mode."""

from __future__ import annotations

import logging
import re

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from mas_client import MASClient, MASResult

logger = logging.getLogger(__name__)


class SlackMASBot:
    def __init__(self, bot_token: str, app_token: str, mas_client: MASClient):
        self.mas_client = mas_client
        self.app_token = app_token

        # slack_thread_ts -> list of message dicts (conversation history for MAS)
        self.conversation_map: dict[str, list[dict]] = {}

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
            if event.get("channel_type") != "im" and "thread_ts" not in event:
                return
            if event.get("bot_id") or event.get("subtype"):
                return
            question = event.get("text", "").strip()
            thread_ts = event.get("thread_ts") or event.get("ts", "")
            self._handle_question(question, thread_ts, say)

        @self.app.action("thumbs_up")
        def handle_thumbs_up(ack, body, client):
            ack()
            self._handle_feedback(body, client, ":+1:")

        @self.app.action("thumbs_down")
        def handle_thumbs_down(ack, body, client):
            ack()
            self._handle_feedback(body, client, ":-1:")

    def _handle_question(self, question: str, thread_ts: str, say) -> None:
        if not question:
            say(
                text="Ask me a question! e.g. `@GenieBot how many active players are in NA?`",
                thread_ts=thread_ts,
            )
            return

        thinking = say(text=":hourglass: Thinking...", thread_ts=thread_ts)
        thinking_ts = thinking["ts"]
        channel = thinking["channel"]

        try:
            # Get or create conversation history for this thread
            history = self.conversation_map.get(thread_ts, [])
            result = self.mas_client.ask_question(question, history)

            # Store updated history
            if result.status == "COMPLETED":
                history.append({"role": "user", "content": question})
                history.append({"role": "assistant", "content": result.content})
                self.conversation_map[thread_ts] = history

            blocks = self._format_result_blocks(result)
            text = result.content or "Query complete"

            self.app.client.chat_update(
                channel=channel, ts=thinking_ts, text=text, blocks=blocks,
            )

        except Exception as e:
            logger.exception("Error handling question")
            self.app.client.chat_update(
                channel=channel,
                ts=thinking_ts,
                text=f":warning: Error: {e}",
                blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": f":warning: *Error:* {e}"}}],
            )

    def _handle_feedback(self, body: dict, client, emoji: str) -> None:
        message_ts = body.get("message", {}).get("ts", "")
        user = body.get("user", {}).get("id", "someone")

        original_blocks = body.get("message", {}).get("blocks", [])
        updated_blocks = [b for b in original_blocks if b.get("type") != "actions"]
        updated_blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"{emoji} Feedback from <@{user}>"}],
        })

        client.chat_update(
            channel=body["channel"]["id"],
            ts=message_ts,
            text=body.get("message", {}).get("text", ""),
            blocks=updated_blocks,
        )

    def _format_result_blocks(self, result: MASResult) -> list[dict]:
        blocks: list[dict] = []

        if result.status == "COMPLETED" and result.content:
            # Split long content into multiple blocks if needed (Slack 3000 char limit)
            content = result.content
            while content:
                chunk = content[:2900]
                content = content[2900:]
                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": chunk},
                })

        elif result.status == "FAILED":
            error_msg = result.error or "Could not answer this question. Try rephrasing."
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f":x: {error_msg}"},
            })
            return blocks

        # Feedback buttons
        blocks.append({
            "type": "actions",
            "elements": [
                {"type": "button", "text": {"type": "plain_text", "text": ":+1:", "emoji": True}, "action_id": "thumbs_up"},
                {"type": "button", "text": {"type": "plain_text", "text": ":-1:", "emoji": True}, "action_id": "thumbs_down"},
            ],
        })

        return blocks
