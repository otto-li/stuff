"""
Pokemon TCG Live Multi-Agent Supervisor - Models from Code format.

Uses mlflow.deployments client for LLM calls (auto-authenticated in serving)
and raw HTTP with mlflow credential extraction for Genie API calls.
"""

import json
import logging
import os
import time

import mlflow

logger = logging.getLogger(__name__)

GENIE_SPACES = [
    {"id": "01f12e035f401a8b89eac30662945c77", "name": "Player Accounts & Profiles", "description": "Player accounts, demographics, subscription tiers, ranks, churn risk, collection stats, elo ratings, friend counts, tournaments"},
    {"id": "01f12e035f7b1cb2bb1bc38b3c57aa39", "name": "Player Next Best Action", "description": "ML-driven recommendations, offers, engagement actions, retention actions, upsell, reactivation, segment targeting, LTV impact, offer codes, priority scores"},
    {"id": "01f12e035fa61be2baf41022472e475a", "name": "Player Activities & Engagement", "description": "Daily activities, match history, pack openings, store purchases, card trades, tournaments, session patterns, gems/coins, XP, device types, app versions"},
]

ROUTER_PROMPT = """You are a query router for Pokemon TCG Live data. Given a user question, pick the most appropriate data space.

Available spaces:
{spaces}

Respond with ONLY the space ID (the hex string). Nothing else."""

class PokemonTCGSupervisor(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        self.spaces = GENIE_SPACES
        self.llm_model = os.environ.get("LLM_ENDPOINT", "databricks-claude-sonnet-4")
        self._deploy_client = None

    def _get_deploy_client(self):
        if self._deploy_client is None:
            self._deploy_client = mlflow.deployments.get_deploy_client("databricks")
        return self._deploy_client

    def _route_question(self, question):
        spaces_text = "\n".join(f"- ID: {s['id']} | Name: {s['name']} | Topics: {s['description']}" for s in self.spaces)
        try:
            client = self._get_deploy_client()
            response = client.predict(
                endpoint=self.llm_model,
                inputs={
                    "messages": [
                        {"role": "system", "content": ROUTER_PROMPT.format(spaces=spaces_text)},
                        {"role": "user", "content": question},
                    ],
                    "temperature": 0.0,
                    "max_tokens": 100,
                },
            )
            chosen_id = response["choices"][0]["message"]["content"].strip()
            for s in self.spaces:
                if s["id"] in chosen_id:
                    return s
        except Exception as e:
            logger.warning("Routing failed: %s", e)
        return self.spaces[0]

    def _ask_genie(self, space_id, question):
        """Ask Genie via mlflow deploy client's predict (treats Genie as an agent endpoint)."""
        try:
            client = self._get_deploy_client()
            # Use the Genie space as an agent endpoint via the deploy client
            # This uses the same credential injection as LLM calls
            response = client.predict(
                endpoint=f"genie-space-{space_id[:8]}",  # Won't work - Genie isn't a serving endpoint
                inputs={"content": question},
            )
            return {"status": "COMPLETED", "narrative": str(response)}
        except Exception:
            pass

        # Fallback: use the SDK with explicit credential extraction
        try:
            from mlflow.utils.databricks_utils import get_databricks_host_creds
            import requests as req_lib

            creds = get_databricks_host_creds()
            host = creds.host.rstrip("/") if creds.host else "https://fe-vm-otto-demo.cloud.databricks.com"
            headers = {"Content-Type": "application/json"}
            if creds.token:
                headers["Authorization"] = f"Bearer {creds.token}"

            # Start conversation
            resp = req_lib.post(f"{host}/api/2.0/genie/spaces/{space_id}/start-conversation",
                json={"content": question}, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            cid = data.get("conversation_id", "")
            mid = data.get("message_id", "")

            # Poll for completion
            status = "PENDING"
            msg = {}
            for _ in range(90):
                time.sleep(2)
                poll = req_lib.get(
                    f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{cid}/messages/{mid}",
                    headers=headers, timeout=30)
                poll.raise_for_status()
                msg = poll.json()
                status = msg.get("status", "")
                if status in ("COMPLETED", "FAILED"):
                    break

            result = {"status": status, "narrative": "", "sql": "", "data": [], "columns": [], "suggestions": []}
            if msg.get("error"):
                result["error"] = str(msg["error"])

            for att in msg.get("attachments", []):
                text = att.get("text", {})
                if text and text.get("content"):
                    result["narrative"] = text["content"]
                query = att.get("query", {})
                if query and query.get("query"):
                    result["sql"] = query["query"]
                att_id = att.get("attachment_id") or att.get("id")
                if query and att_id:
                    try:
                        qr_resp = req_lib.get(
                            f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{cid}/messages/{mid}/attachments/{att_id}/query-result",
                            headers=headers, timeout=30)
                        qr_resp.raise_for_status()
                        qr = qr_resp.json()
                        sr = qr.get("statement_response", {})
                        columns = sr.get("manifest", {}).get("schema", {}).get("columns", [])
                        if columns:
                            result["columns"] = [c["name"] for c in columns]
                        data_array = sr.get("result", {}).get("data_array", [])
                        if data_array:
                            result["data"] = [[str(c) if c is not None else "" for c in row] for row in data_array[:20]]
                    except Exception:
                        pass
                for sq in att.get("suggested_questions", []):
                    q = sq.get("question") or sq.get("text")
                    if q:
                        result["suggestions"].append(q)

            return result
        except Exception as e:
            return {"status": "FAILED", "error": str(e)}

    def _format_response(self, space, gr):
        parts = []
        if gr.get("narrative"):
            parts.append(gr["narrative"])
        if gr.get("sql"):
            parts.append(f"\n**SQL Query:**\n```sql\n{gr['sql']}\n```")
        if gr.get("columns") and gr.get("data"):
            cols, rows = gr["columns"], gr["data"]
            parts.append(f"\n**Results:**\n| {' | '.join(cols)} |\n| {' | '.join('---' for _ in cols)} |\n" + "\n".join("| " + " | ".join(r) + " |" for r in rows[:15]))
            if len(rows) > 15:
                parts.append(f"\n*Showing 15 of {len(rows)} rows*")
        if gr.get("suggestions"):
            parts.append(f"\n**Suggestions:** {', '.join(gr['suggestions'][:3])}")
        parts.append(f"\n*Answered by: {space['name']}*")
        if gr.get("status") == "FAILED":
            return f"Error: {gr.get('error', '?')}\n*Routed to: {space['name']}*"
        return "\n".join(parts) if parts else "No results."

    def predict(self, context, model_input, params=None):
        import pandas as pd
        if isinstance(model_input, pd.DataFrame):
            recs = model_input.to_dict(orient="records")
            messages = recs[0].get("messages", []) if recs else []
            if isinstance(messages, str):
                messages = json.loads(messages)
        elif isinstance(model_input, dict):
            messages = model_input.get("messages", [])
        else:
            messages = model_input if isinstance(model_input, list) else []
        question = next((m["content"] for m in reversed(messages) if m.get("role") == "user"), "")
        if not question:
            return {"content": "Ask me about Pokemon TCG Live player data!"}
        space = self._route_question(question)
        gr = self._ask_genie(space["id"], question)
        return {"content": self._format_response(space, gr)}


mlflow.models.set_model(PokemonTCGSupervisor())
