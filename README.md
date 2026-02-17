# 🚧 Advertiser Segment Builder (UNFINISHED)

A Databricks App for creating advertiser segments with AI-powered analytics.

**Status:** 🔴 Incomplete - Database integration needs work

## Overview

Dark-themed streaming service aesthetic (7plus-inspired) for building targeted audience segments with predictive analytics.

### Features Implemented
- ✅ Dark theme UI with red accents (#e50914)
- ✅ FastAPI backend with dual-mode authentication
- ✅ Unity Catalog medallion architecture (bronze → silver → gold)
- ✅ Segment creation with age bands, demographics, locations, interests
- ✅ Analytics dashboard with Chart.js visualizations
- ✅ Foundation Model API integration for predictions
- ✅ Deployed to Databricks Apps

### Tech Stack
- **Frontend:** Pure HTML/CSS/JavaScript (no build process)
- **Backend:** FastAPI + aiohttp
- **Database:** Unity Catalog (`otto_demo.ad_segments` schema)
- **AI:** Databricks Foundation Model API (Claude Sonnet 4.5)
- **Deployment:** Databricks Apps on fe-vm-otto-demo

## Architecture

### Unity Catalog Schema
```
otto_demo.ad_segments
├── bronze_impressions      (raw impression data)
├── silver_user_profiles    (cleaned user profiles)
├── gold_segments           (advertiser segments)
└── gold_segment_analytics  (analytics metrics)
```

### App Structure
```
advertiser-segments-app/
├── app.py                  # FastAPI entry point
├── app.yaml                # Databricks Apps config
├── requirements.txt        # Python dependencies
├── server/
│   ├── config.py          # Dual-mode auth
│   ├── db.py              # Unity Catalog REST API client
│   ├── llm.py             # Foundation Model API
│   └── routes/
│       └── segments.py    # API endpoints
└── frontend/static/
    ├── index.html         # Main UI
    ├── css/styles.css     # Dark theme styling
    └── js/app.js          # Client-side logic
```

## Current Issues

⚠️ **Database writes from app not working** - See TODO.md for details

The app runs in "demo mode" generating synthetic data. Manual SQL writes to Unity Catalog work correctly, but the app's REST API calls fail silently.

## Deployment

**Current Deployment:**
- URL: https://advertiser-segments-2198414303818321.aws.databricksapps.com
- Workspace: fe-vm-otto-demo
- Profile: `fe-vm-otto-demo`

## Quick Start (Local Development)

```bash
# Set up environment
export DATABRICKS_PROFILE=fe-vm-otto-demo

# Start backend
uv run uvicorn app:app --reload --port 8000

# Open in browser
open http://localhost:8000
```

## Screenshots

The app features a dark theme with red accent colors matching 7plus.com.au aesthetic.

## See Also

- [TODO.md](./TODO.md) - Remaining tasks
- [Databricks Apps Docs](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/)
