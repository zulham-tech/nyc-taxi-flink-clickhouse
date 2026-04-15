# NYC Taxi Real-Time Analytics | Apache Flink, ClickHouse

**Stack:** NYC TLC API -> Kafka -> Apache Flink -> ClickHouse -> Airflow

## Key Metrics
- Sub-100ms aggregation on millions of rows
- 2,000 trips/hour ingested continuously
- 5-min tumbling window per pickup location
- ClickHouse vs PostgreSQL: seconds -> milliseconds for GROUP BY queries

## Architecture
```
NYC TLC Socrata API (free, no key, 1M+ trips/month)
    |
Kafka [nyc-taxi-trips, GZIP]
    |
Flink SQL: 5-min tumbling window per pickup_location
    -> trip_count, avg_fare, avg_tip_rate, total_revenue
    |
ClickHouse MergeTree columnar OLAP
    taxi_trips (raw) + trip_5min_agg (Flink)
```

## Tech Stack
Python, Apache Kafka, Apache Flink 1.18, ClickHouse, Airflow, Docker

## Author
Ahmad Zulham Hamdan | https://linkedin.com/in/ahmad-zulham-665170279
