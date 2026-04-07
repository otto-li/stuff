"""
Pokemon TCG Live Multi-Agent Supervisor.

Routes user questions to the appropriate Genie space based on topic.
Deployed as an MLflow PyFunc model to a Databricks serving endpoint.
"""

import json
import logging
import os

import mlflow

logger = logging.getLogger(__name__)

# Genie space configuration
GENIE_SPACES = [
    {
        "id": "01f12e035f401a8b89eac30662945c77",
        "name": "Player Accounts & Profiles",
        "description": "Player accounts, demographics, subscription tiers, ranks, churn risk, collection stats, elo ratings, friend counts, tournaments",
    },
    {
        "id": "01f12e035f7b1cb2bb1bc38b3c57aa39",
        "name": "Player Next Best Action",
        "description": "ML-driven recommendations, offers, engagement actions, retention actions, upsell, reactivation, segment targeting, LTV impact, offer codes, priority scores",
    },
    {
        "id": "01f12e035fa61be2baf41022472e475a",
        "name": "Player Activities & Engagement",
        "description": "Daily activities, match history, pack openings, store purchases, card trades, tournaments, session patterns, gems/coins, XP, device types, app versions",
    },
]

ROUTER_PROMPT = """You are a query router for Pokemon TCG Live data. Given a user question, pick the most appropriate data space to answer it.

Available spaces:
{spaces}

Respond with ONLY the space ID (the hex string) that best matches the question. Nothing else."""

SYNTHESIS_PROMPT = """You are a helpful Pokemon TCG Live data assistant. A Genie AI/BI agent answered the user's question. Present the answer clearly and concisely.

The answer came from the "{space_name}" data space.

Genie's response:
{genie_response}

If the response includes SQL results or data, present them in a clear format. If the response indicates an error, explain it helpfully and suggest rephrasing."""


class PokemonTCGSupervisor(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        """Initialize clients on model load."""
        from databricks.sdk import WorkspaceClient
        from openai import OpenAI

        self.w = WorkspaceClient()
        host = self.w.config.host.rstrip("/")
        token = self.w.config.authenticate()

        # LLM client for routing
        self.llm = OpenAI(
            base_url=f"{host}/serving-endpoints",
            api_key=token,
        )

        self.llm_model = os.environ.get("LLM_ENDPOINT", "databricks-claude-sonnet-4")
        self.spaces = GENIE_SPACES
        logger.info("Supervisor loaded with %d Genie spaces", len(self.spaces))

    def _route_question(self, question: str) -> dict:
        """Use LLM to pick the best Genie space for the question."""
        spaces_text = "\n".join(
            f"- ID: {s['id']} | Name: {s['name']} | Topics: {s['description']}"
            for s in self.spaces
        )
        prompt = ROUTER_PROMPT.format(spaces=spaces_text)

        try:
            response = self.llm.chat.completions.create(
                model=self.llm_model,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": question},
                ],
                temperature=0.0,
                max_tokens=100,
            )
            chosen_id = response.choices[0].message.content.strip()

            # Find matching space
            for s in self.spaces:
                if s["id"] in chosen_id:
                    return s

        except Exception as e:
            logger.warning("Routing failed: %s, using default space", e)

        # Default to first space
        return self.spaces[0]

    def _ask_genie(self, space_id: str, question: str) -> dict:
        """Ask a Genie space a question and return the result."""
        try:
            wait = self.w.genie.start_conversation(
                space_id=space_id, content=question
            )
            conversation_id = wait.response.conversation_id
            message_id = wait.response.message_id
            from datetime import timedelta
            msg = wait.result(timeout=timedelta(minutes=3))

            result = {
                "status": msg.status.value if msg.status else "UNKNOWN",
                "narrative": "",
                "sql": "",
                "data": [],
                "suggestions": [],
            }

            if msg.error:
                result["error"] = str(msg.error)

            if msg.attachments:
                for att in msg.attachments:
                    if att.text and att.text.content:
                        result["narrative"] = att.text.content
                    if att.query and att.query.query:
                        result["sql"] = att.query.query

                    att_id = getattr(att, "attachment_id", None) or getattr(att, "id", None)
                    if att.query and att_id:
                        try:
                            qr = self.w.genie.get_message_attachment_query_result(
                                space_id=space_id,
                                conversation_id=conversation_id,
                                message_id=message_id,
                                attachment_id=att_id,
                            )
                            if qr.statement_response:
                                sr = qr.statement_response
                                if sr.manifest and sr.manifest.schema and sr.manifest.schema.columns:
                                    cols = [c.name for c in sr.manifest.schema.columns]
                                    result["columns"] = cols
                                if sr.result and sr.result.data_array:
                                    result["data"] = [
                                        [str(c) if c is not None else "" for c in row]
                                        for row in sr.result.data_array[:20]
                                    ]
                        except Exception as e:
                            logger.debug("Could not fetch query result: %s", e)

                    for sq in getattr(att, "suggested_questions", None) or []:
                        q = getattr(sq, "question", None) or getattr(sq, "text", None)
                        if q:
                            result["suggestions"].append(q)

            return result

        except Exception as e:
            logger.exception("Genie query failed")
            return {"status": "FAILED", "error": str(e)}

    def _format_response(self, space: dict, genie_result: dict) -> str:
        """Format the Genie result into a readable response."""
        parts = []

        if genie_result.get("narrative"):
            parts.append(genie_result["narrative"])

        if genie_result.get("sql"):
            parts.append(f"\n**SQL Query:**\n```sql\n{genie_result['sql']}\n```")

        if genie_result.get("columns") and genie_result.get("data"):
            cols = genie_result["columns"]
            rows = genie_result["data"]
            # Format as markdown table
            header = "| " + " | ".join(cols) + " |"
            separator = "| " + " | ".join("---" for _ in cols) + " |"
            body = "\n".join("| " + " | ".join(row) + " |" for row in rows[:15])
            parts.append(f"\n**Results:**\n{header}\n{separator}\n{body}")
            if len(rows) > 15:
                parts.append(f"\n*Showing 15 of {len(rows)} rows*")

        if genie_result.get("suggestions"):
            suggestions = ", ".join(f'"{s}"' for s in genie_result["suggestions"][:3])
            parts.append(f"\n**Follow-up suggestions:** {suggestions}")

        parts.append(f"\n*Answered by: {space['name']}*")

        if genie_result.get("status") == "FAILED":
            error = genie_result.get("error", "Unknown error")
            return f"I wasn't able to answer that question. Error: {error}\n\nTry rephrasing your question.\n\n*Routed to: {space['name']}*"

        return "\n".join(parts) if parts else "No results found. Try rephrasing your question."

    def predict(self, context, model_input, params=None):
        """Handle incoming chat messages."""
        import pandas as pd

        # Extract messages from input
        if isinstance(model_input, pd.DataFrame):
            messages = model_input.to_dict(orient="records")
            if len(messages) > 0 and "messages" in messages[0]:
                messages = json.loads(messages[0]["messages"]) if isinstance(messages[0]["messages"], str) else messages[0]["messages"]
        elif isinstance(model_input, dict):
            messages = model_input.get("messages", [])
        elif isinstance(model_input, list):
            messages = model_input
        else:
            messages = []

        # Get the last user message
        question = ""
        for msg in reversed(messages):
            if msg.get("role") == "user":
                question = msg.get("content", "")
                break

        if not question:
            return {"content": "Please ask a question about Pokemon TCG Live player data!"}

        # Route and query
        space = self._route_question(question)
        logger.info("Routing to space: %s (%s)", space["name"], space["id"])

        genie_result = self._ask_genie(space["id"], question)
        response = self._format_response(space, genie_result)

        return {"content": response}
