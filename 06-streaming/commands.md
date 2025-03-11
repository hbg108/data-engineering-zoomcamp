# 06-streaming

## Check if Make is installed

```bash
make --version
```

### Result

installed

## Setup

```bash
cd 06-streaming/pyflink
docker-compose up
```

## Run query

```sql
CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);

CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP,
    test_data INTEGER,
    num_hits BIGINT,
    PRIMARY KEY (event_hour, test_data)
);

CREATE TABLE taxi_events (
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    num_of_trips INTEGER
);
```

## Run producer

```bash
conda install kafka-python
```

```bash
python producer.py
```

## Start Flink job

```bash
make job
make aggregation_job
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/session_job.py --pyFiles /opt/src -d
```

flink 1.20.1