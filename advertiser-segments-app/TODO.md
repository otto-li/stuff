# TODO - Advertiser Segment Builder

## 🔴 Critical Issues

### 1. Fix Database Write Authentication

**Problem:** App cannot write to Unity Catalog tables. Manual SQL statements work, but REST API calls from the app fail silently.

**Possible Causes:**
- OAuth token refresh timing issue
- Service principal permissions in Databricks Apps environment
- Incorrect warehouse ID retrieval
- CORS or network policy blocking internal API calls

**Steps to Debug:**
1. Add detailed logging to `server/db.py` execute_sql method
2. Check app logs at `/logz` for authentication errors
3. Verify service principal has USAGE on catalog and schema
4. Test with hardcoded warehouse ID vs dynamic lookup
5. Add retry logic with exponential backoff

**Code Location:** `server/db.py` lines 13-60

### 2. Add Lakebase Resource (Optional Alternative)

Instead of fixing REST API, could use Lakebase PostgreSQL:

**Steps:**
1. Go to: https://fe-vm-otto-demo.cloud.databricks.com/compute/apps?o=2198414303818321
2. Click **advertiser-segments** → **Edit configuration**
3. Scroll to **Resources** → Click **Add resource**
4. Select **Database** (Lakebase)
5. Choose existing Lakebase instance or create new
6. Set permission: **Can connect**
7. Update `server/db.py` to use Lakebase instead of Unity Catalog
8. Redeploy

## 🟡 Medium Priority

### 3. Improve Error Handling

- [ ] Replace `try/except` blocks that swallow errors
- [ ] Add structured logging (not just print statements)
- [ ] Return error details to frontend instead of falling back to demo mode
- [ ] Add health check endpoint that verifies database connectivity

### 4. Add Data Validation

- [ ] Validate segment names (SQL injection prevention)
- [ ] Validate array inputs aren't empty
- [ ] Add input sanitization for all user inputs
- [ ] Add request/response models with Pydantic

### 5. Enhance Analytics

- [ ] Store actual impression data in bronze table
- [ ] Implement silver table ETL (data cleaning)
- [ ] Create scheduled jobs to update analytics
- [ ] Add time-series forecasting with better models

## 🟢 Nice to Have

### 6. UI Improvements

- [ ] Add loading states and spinners
- [ ] Add error toast notifications
- [ ] Add form validation feedback
- [ ] Add ability to edit/delete segments
- [ ] Add segment comparison view
- [ ] Make charts interactive (zoom, pan, filter)

### 7. Features

- [ ] User authentication and multi-tenancy
- [ ] Export segments to CSV/JSON
- [ ] Schedule segment reports
- [ ] A/B testing for segments
- [ ] Integration with ad platforms (Google Ads, Facebook Ads)

### 8. Testing

- [ ] Add unit tests for API endpoints
- [ ] Add integration tests for database operations
- [ ] Add end-to-end tests with Playwright
- [ ] Add load testing

### 9. Documentation

- [ ] Add API documentation with FastAPI's auto-docs
- [ ] Create video walkthrough
- [ ] Document deployment process
- [ ] Add troubleshooting guide

## 📝 Notes

**Working Manually:**
```bash
# Manual insert that works:
databricks api post /api/2.0/sql/statements/ --json='{
  "statement": "INSERT INTO otto_demo.ad_segments.gold_segments VALUES (...)",
  "warehouse_id": "3baa12157046a0c0",
  "catalog": "otto_demo",
  "schema": "ad_segments"
}' -p fe-vm-otto-demo
```

**App Code That Fails:**
See `server/db.py` execute_sql method - uses same REST API but doesn't work.

## 🎯 Quick Wins

Start with these to get database working:

1. **Add logging:** Print full request/response in execute_sql
2. **Check logs:** Look for 401/403 errors at /logz
3. **Test token:** Verify get_oauth_token() returns valid token in app context
4. **Hardcode warehouse:** Replace dynamic lookup with "3baa12157046a0c0"
