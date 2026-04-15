# NYC Taxi Real-Time Analytics | Kafka Â· Apache Flink Â· ClickHouse

> **Type:** Streaming | **Stack:** NYC TLC API â†’ Kafka â†’ Apache Flink â†’ ClickHouse â†’ Airflow

## Key Metrics
- **Sub-100ms** aggregation on millions of rows
- **2,000 trips/hour** ingested continuously
- **5-min tumbling window** per pickup location
- ClickHouse vs PostgreSQL: seconds â†’ milliseconds for GROUP BY queries

## Architecture
```
NYC TLC Socrata API (free Â· no key Â· 1M+ trips/month)
    â†“
Kafka [nyc-taxi-trips Â· GZIP]
    â†“ (Apache Flink SQL)
Flink: 5-min tumbling window per pickup_location
    â†’ trip_count Â· avg_fare Â· avg_tip_rate Â· total_revenue
    â†“
ClickHouse (MergeTree columnar OLAP)
    taxi_trips (raw) + trip_5min_agg (Flink)
```

## Tech Stack
Python Â· Apache Kafka Â· Apache Flink 1.18 Â· ClickHouse Â· Airflow Â· Docker

## Author
**Ahmad Zulham Hamdan** | [LinkedIn](https://linkedin.com/in/ahmad-zulham-hamdan-665170279) | [GitHub](https://github.com/zulham-tech)
