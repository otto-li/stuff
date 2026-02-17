"""Unity Catalog database operations using REST API."""
from .config import get_workspace_host, get_oauth_token
import aiohttp
import asyncio
from typing import List, Dict, Any
import json

class UnityCatalog:
    def __init__(self):
        self.catalog = "otto_demo"
        self.schema = "ad_segments"
        self._warehouse_id = None

    async def execute_sql(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL query against Unity Catalog using REST API."""
        try:
            if not self._warehouse_id:
                self._warehouse_id = await self._get_warehouse_id()

            host = get_workspace_host()
            token = get_oauth_token()

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }

            payload = {
                "statement": sql,
                "warehouse_id": self._warehouse_id,
                "catalog": self.catalog,
                "schema": self.schema,
                "format": "JSON_ARRAY",
                "wait_timeout": "30s"
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{host}/api/2.0/sql/statements/",
                    headers=headers,
                    json=payload
                ) as response:
                    result = await response.json()

                    if response.status != 200:
                        print(f"SQL execution error: {result}")
                        return []

                    # Parse results
                    if result.get("result") and result["result"].get("data_array"):
                        if result.get("manifest") and result["manifest"].get("schema"):
                            columns = [col["name"] for col in result["manifest"]["schema"]["columns"]]
                            rows = []
                            for row_data in result["result"]["data_array"]:
                                row = {}
                                for idx, col in enumerate(columns):
                                    row[col] = row_data[idx]
                                rows.append(row)
                            return rows
                    return []
        except Exception as e:
            print(f"Database execute_sql error: {e}")
            return []

    async def _get_warehouse_id(self) -> str:
        """Get first available SQL warehouse using REST API."""
        try:
            host = get_workspace_host()
            token = get_oauth_token()

            headers = {"Authorization": f"Bearer {token}"}

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{host}/api/2.0/sql/warehouses",
                    headers=headers
                ) as response:
                    result = await response.json()
                    warehouses = result.get("warehouses", [])
                    if warehouses:
                        return warehouses[0]["id"]

            raise Exception("No SQL warehouses available")
        except Exception as e:
            print(f"Error getting warehouse: {e}")
            # Fallback to known warehouse
            return "3baa12157046a0c0"
    
    async def create_schema_if_not_exists(self):
        """Create schema for advertiser segments if it doesn't exist."""
        sql = f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}"
        await self.execute_sql(sql)
    
    async def create_bronze_table(self):
        """Create bronze table for raw impression data."""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.bronze_impressions (
            impression_id STRING,
            timestamp TIMESTAMP,
            user_id STRING,
            age_band STRING,
            demographic STRING,
            location STRING,
            interest STRING,
            device STRING,
            engagement_minutes DOUBLE,
            ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        ) USING DELTA
        """
        await self.execute_sql(sql)
    
    async def create_silver_table(self):
        """Create silver table for cleaned and deduplicated data."""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.silver_user_profiles (
            user_id STRING PRIMARY KEY,
            age_band STRING,
            demographic STRING,
            location STRING,
            interests ARRAY<STRING>,
            devices ARRAY<STRING>,
            avg_engagement_minutes DOUBLE,
            total_impressions BIGINT,
            last_seen TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        ) USING DELTA
        """
        await self.execute_sql(sql)
    
    async def create_gold_segments_table(self):
        """Create gold table for advertiser segments."""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.gold_segments (
            segment_id STRING PRIMARY KEY,
            segment_name STRING,
            age_bands ARRAY<STRING>,
            demographics ARRAY<STRING>,
            locations ARRAY<STRING>,
            interests ARRAY<STRING>,
            min_engagement_minutes DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            created_by STRING,
            estimated_reach BIGINT
        ) USING DELTA
        """
        await self.execute_sql(sql)
    
    async def create_gold_analytics_table(self):
        """Create gold table for segment analytics."""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.gold_segment_analytics (
            analytics_id STRING PRIMARY KEY,
            segment_id STRING,
            metric_date DATE,
            impressions BIGINT,
            engagement_minutes DOUBLE,
            unique_users BIGINT,
            top_devices ARRAY<STRUCT<device: STRING, count: BIGINT>>,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        ) USING DELTA
        """
        await self.execute_sql(sql)
    
    async def initialize_tables(self):
        """Initialize all medallion architecture tables."""
        await self.create_schema_if_not_exists()
        await self.create_bronze_table()
        await self.create_silver_table()
        await self.create_gold_segments_table()
        await self.create_gold_analytics_table()

# Global instance
db = UnityCatalog()
