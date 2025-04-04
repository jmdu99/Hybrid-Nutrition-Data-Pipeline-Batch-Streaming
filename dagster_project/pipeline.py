import os
import json
import time
import openai
from dagster import op, job, schedule, Definitions
from cassandra.cluster import Cluster
from clickhouse_driver import Client

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "nutrition_ks")
TABLE = os.environ.get("CASSANDRA_TABLE", "items_raw")

@op
def extract_from_cassandra():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect("nutrition_ks")
    # Select all rows and filter out rows with empty data in Python
    rows = session.execute("SELECT item_name, ingestion_ts, data FROM items_raw ALLOW FILTERING")
    results = []
    for row in rows:
        try:
            data_value = json.loads(row.data)
        except Exception:
            data_value = None
        if not data_value:
            continue
        results.append({
            "item_name": row.item_name,
            "ingestion_ts": row.ingestion_ts,
            "data": row.data
        })
    return results

@op
def transform_with_openai(records):
    enriched = []
    for record in records:
        item_name = record["item_name"]
        raw_json = record["data"]
        try:
            data_list = json.loads(raw_json) or []
            if not data_list:
                continue
            data_fields = data_list[0]
            for key, value in data_fields.items():
                if key == "name":
                    continue
                record[key] = value
            calories = data_list[0].get("calories", 0)
            prompts = {
                "openai_description": f"Provide a short nutritional description for '{item_name}' with approximately {calories} calories.",
                "openai_best_pairings": f"Suggest foods that pair well with '{item_name}'.",
                "openai_health_impact": f"Explain the health impact of '{item_name}' considering keto, vegan, and low-sodium diets.",
                "openai_preparation_tips": f"Give a useful tip on how to prepare or consume '{item_name}' for better taste or nutritional benefits."
            }
            for key, prompt in prompts.items():
                response = openai.ChatCompletion.create(
                    messages=[{"role": "user", "content": prompt}],
                    model="gpt-4o"
                )
                record[key] = response.choices[0].message.content.strip() if response.choices else ""
                time.sleep(2)
            enriched.append(record)
        except Exception as e:
            print(f"OpenAI error for {item_name}: {e}")
    return enriched

@op
def load_into_clickhouse(enriched):
    ch_client = Client(host=CLICKHOUSE_HOST)
    ch_client.execute("CREATE DATABASE IF NOT EXISTS data_ck")
    ch_client.execute("""
        CREATE TABLE IF NOT EXISTS data_ck.items_enriched (
            item_name String,
            ingestion_ts String,
            calories Float64,
            serving_size_g Float64,
            fat_total_g Float64,
            fat_saturated_g Float64,
            protein_g Float64,
            sodium_mg Float64,
            potassium_mg Float64,
            cholesterol_mg Float64,
            carbohydrates_total_g Float64,
            fiber_g Float64,
            sugar_g Float64,
            openai_description String,
            openai_best_pairings String,
            openai_health_impact String,
            openai_preparation_tips String
        ) ENGINE = MergeTree()
        ORDER BY (item_name, ingestion_ts)
    """)
    rows = []
    for rec in enriched:
        rows.append((
            str(rec["item_name"]),
            str(rec["ingestion_ts"]),
            float(rec.get("calories", 0)),
            float(rec.get("serving_size_g", 0)),
            float(rec.get("fat_total_g", 0)),
            float(rec.get("fat_saturated_g", 0)),
            float(rec.get("protein_g", 0)),
            float(rec.get("sodium_mg", 0)),
            float(rec.get("potassium_mg", 0)),
            float(rec.get("cholesterol_mg", 0)),
            float(rec.get("carbohydrates_total_g", 0)),
            float(rec.get("fiber_g", 0)),
            float(rec.get("sugar_g", 0)),
            str(rec.get("openai_description", "")),
            str(rec.get("openai_best_pairings", "")),
            str(rec.get("openai_health_impact", "")),
            str(rec.get("openai_preparation_tips", ""))
        ))
    ch_client.execute("""
        INSERT INTO data_ck.items_enriched (
            item_name, ingestion_ts, calories, serving_size_g, fat_total_g, fat_saturated_g,
            protein_g, sodium_mg, potassium_mg, cholesterol_mg, carbohydrates_total_g, fiber_g,
            sugar_g, openai_description, openai_best_pairings, openai_health_impact, openai_preparation_tips
        )
        VALUES
    """, rows)
    
    # After successful insertion, mark processed rows in Cassandra
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect("nutrition_ks")
    for rec in enriched:
        session.execute("UPDATE items_raw SET data = '[]' WHERE item_name = %s", (rec["item_name"],))
    return f"Inserted {len(rows)} records into ClickHouse and marked as processed in Cassandra."

@job
def data_pipeline():
    data = extract_from_cassandra()
    enriched = transform_with_openai(data)
    load_into_clickhouse(enriched)

@schedule(
    cron_schedule="*/10 * * * *",
    job=data_pipeline,
    execution_timezone="UTC"
)
def data_pipeline_every_10_minutes(_context):
    return {}

defs = Definitions(
    jobs=[data_pipeline],
    schedules=[data_pipeline_every_10_minutes],
)
