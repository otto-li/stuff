# Bot Detection Demo

## Overview

Real-time bot detection for digital advertising and web traffic. Advertisers and
publishers lose significant revenue to bot traffic (fake clicks, inflated impressions).
This demo shows how Databricks enables a full ML lifecycle for bot detection — from
feature engineering through to real-time inference via a model serving endpoint,
with a live Databricks App that simulates a user sign-up flow.

## Business Context

- **Use Case**: Detect bot vs. human web sessions in real-time to protect ad spend
- **Personas**: Data Engineer, ML Engineer, Platform Team
- **Value**: Prevent ~20-40% ad budget waste from invalid traffic; protect brand safety

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Databricks Platform                          │
│                                                                       │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────────────┐   │
│  │  Raw Traffic │    │  Feature     │    │  MLflow + Unity      │   │
│  │  Delta Table │───▶│  Engineering │───▶│  Catalog Registry    │   │
│  │  (500K rows) │    │  (Serverless)│    │  bot_detector v1     │   │
│  └─────────────┘    └──────────────┘    └──────────┬───────────┘   │
│                             │                        │               │
│                    ┌────────▼────────┐    ┌──────────▼───────────┐  │
│                    │  Offline Store  │    │  Model Serving       │  │
│                    │  bot_features   │    │  Endpoint            │  │
│                    │  (Delta/UC)     │    │  + AI Gateway        │  │
│                    └────────┬────────┘    └──────────────────────┘  │
│                             │  sync                  ▲               │
│                    ┌────────▼────────┐               │               │
│                    │  Synced Table   │               │               │
│                    │  bot_features   │    ┌──────────┴───────────┐  │
│                    │  _online        │    │  Databricks App      │  │
│                    │  (Lakebase)     │    │  Sign-up Simulator   │  │
│                    └─────────────────┘    └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## Components

| Component | Technology | Location |
|-----------|-----------|----------|
| Raw Data | Delta Table | `otto_demo.bot_detection.raw_traffic` |
| Offline Features | Delta Table (primary key + CDF) | `otto_demo.bot_detection.bot_features` |
| Online Feature Store | Lakebase Synced Table | `otto_demo.bot_detection.bot_features_online` |
| ML Model | XGBoost via MLflow | `otto_demo.bot_detection.bot_detector` v1 |
| Model Serving | Databricks Serving Endpoint + AI Gateway | `bot-detector-endpoint` |
| Inference Log | AI Gateway Inference Tables | `otto_demo.bot_detection.bot_inference*` |
| Demo App | Databricks App (FastAPI + React) | `bot-detection-demo` |

## Key Features Demonstrated

1. **Synthetic data generation** via dbldatagen (500K web sessions, ~35% bots)
2. **Serverless feature engineering** — 22 behavioral features including heuristic composite score
3. **Lakebase Synced Table** — Delta → PostgreSQL sync for sub-millisecond online feature lookup
4. **MLflow** — experiment tracking, model registration in Unity Catalog
5. **Model serving** — REST endpoint with scale-to-zero, AI Gateway inference logging
6. **AI Gateway** — replaces legacy auto_capture_config; logs requests to `otto_demo.bot_detection.bot_inference*`
7. **DAB** — entire ML pipeline as code with `databricks bundle deploy`
8. **Databricks App** — FastAPI + React sign-up simulator with real behavioral signal tracking

## App

**URL**: https://bot-detection-demo-2198414303818321.aws.databricksapps.com

The app simulates a realistic SaaS sign-up flow:
- Tracks mouse events, click count, keystroke timing, session duration in real-time
- On submit: computes 22 features and calls `bot-detector-endpoint`
- Result overlay: red "Bot Detected / Access Denied" or green "Welcome / Account Created"
- "Simulate Bot" button: overrides UA to `python-urllib/3.11`, zeroes mouse events

## Workspace

- **Host**: https://fe-vm-otto-demo.cloud.databricks.com
- **Profile**: fe-vm-otto-demo
- **Catalog**: otto_demo
- **Schema**: bot_detection
