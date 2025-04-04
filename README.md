# Nutrition Project

This project integrates:
- **Kafka** for real-time streaming
- **Producer** that calls **OpenAI** to generate a random food item name
- **Consumer** calls the **Nutrition API** and stores data in **Cassandra**
- **Dagster** to batch-process and enrich data with **OpenAI** again
- **ClickHouse** for analytics
- **Superset** for dashboarding
- **Poetry** installed inside each container, generating its own lockfile

## How it Works

1. **Producer** uses OpenAI to generate a random item name each time, sends it to Kafka.
2. **Consumer** consumes item names, calls Nutrition API, stores raw info in Cassandra.
3. **Dagster** extracts from Cassandra, calls OpenAI for generating additional information, and saves into ClickHouse.
4. **Superset** reads from ClickHouse for dashboarding.
