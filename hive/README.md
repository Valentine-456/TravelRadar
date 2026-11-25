# Hive

HiveServer2 with an embedded metastore: the SQL catalog over the Parquet files on HDFS.

- JDBC: `jdbc:hive2://localhost:10000/travelradar`, web UI http://localhost:10002
- `tables.sql` / `repair.sql` are **generated** from the Spark parsers (`spark/gen_hive_ddl.sh`);
  a unit test fails if they are out of date
- `start-hive.sh` starts HiveServer2, recreates all tables from `tables.sql`, then runs `repair.sql`
  (`MSCK REPAIR TABLE ... SYNC PARTITIONS`) every `HIVE_SYNC_INTERVAL` seconds (300) so new `dt`
  partitions written by Spark become visible

All tables are EXTERNAL: the metastore keeps no state worth backing up, dropping a table never
deletes data. `hive.compute.query.using.stats=false` makes `COUNT(*)` read the files (Spark writes
no Hive statistics).

| Table | Layer | Partitions |
| --- | --- | --- |
| `raw_events` | bronze, every Kafka record | topic, dt (ingest day) |
| `flight_positions`, `ship_positions`, `train_stop_events`, `train_positions`, `vehicle_positions`, `weather_observations` | silver, live | dt (event day) |
| `batch_raw_events`, `train_stop_events_history`, `weather_history` | history backfills | source/dt, dt |
| `source_volume_daily`, `quality_daily`, `consistency_daily`, `rail_delay_daily` | gold, batch views | dt |

```bash
docker compose exec hive beeline -u jdbc:hive2://localhost:10000/travelradar -n hive \
  -e "SELECT dt, topic, records, records_per_min FROM source_volume_daily ORDER BY dt, topic;"
```
