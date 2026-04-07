"""MAS Slack Bot — Socket Mode bot with health endpoint for Databricks Apps."""

import logging
import os
import threading

from dotenv import load_dotenv
from flask import Flask

load_dotenv()

from config import validate_config
from mas_client import MASClient
from slack_bot import SlackMASBot

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
    mas_client = MASClient(config["MAS_ENDPOINT_NAME"])
    bot = SlackMASBot(
        bot_token=config["SLACK_BOT_TOKEN"],
        app_token=config["SLACK_APP_TOKEN"],
        mas_client=mas_client,
    )

    slack_thread = threading.Thread(target=bot.start, daemon=True)
    slack_thread.start()
    logger.info("Slack bot started in background thread")

    port = int(os.environ.get("PORT", 8000))
    logger.info("Starting health server on port %d", port)
    flask_app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
