"""Genie Slack Bot — Socket Mode bot with health endpoint for Databricks Apps."""

import logging
import os
import sys
import threading

from dotenv import load_dotenv
from flask import Flask

load_dotenv()

from config import validate_config
from databricks_genie_client import DatabricksGenieClient
from slack_bot import SlackGenieBot

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

flask_app = Flask(__name__)


@flask_app.route("/", methods=["GET"])
@flask_app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}, 200


def main():
    config = validate_config()
    genie_client = DatabricksGenieClient(config["DATABRICKS_GENIE_SPACE_ID"])
    bot = SlackGenieBot(
        bot_token=config["SLACK_BOT_TOKEN"],
        app_token=config["SLACK_APP_TOKEN"],
        genie_client=genie_client,
    )

    # Run Slack bot in background thread
    slack_thread = threading.Thread(target=bot.start, daemon=True)
    slack_thread.start()
    logger.info("Slack bot started in background thread")

    # Run Flask health server on main thread (Databricks Apps needs an HTTP server)
    port = int(os.environ.get("PORT", 8000))
    logger.info("Starting health server on port %d", port)
    flask_app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
