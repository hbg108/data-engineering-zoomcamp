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
    num_hits INTEGER 
);
```

## Run producer

```bash
conda install kafka-python
```