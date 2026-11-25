"""TravelRadar streaming pipeline: every streaming query in one Spark application.

Per source (see travelradar/sources):
    raw_<source>        Kafka -> HDFS raw/events/topic=<topic>/dt=...      (bronze, 10 min)
    cleaned_<table>     Kafka -> parse, validate, de-duplicate -> HDFS cleaned/<table>/dt=...
                                                                             (silver, 10 min)
and for the map:
    hot_planes          opensky -> Redis planes:*   (10 s)
    hot_ships           ais     -> Redis ships:*    (10 s)

    ./submit.sh -d pipeline_job.py                     # all sources
    ./submit.sh -d pipeline_job.py opensky ais         # a subset
    ./submit.sh -d pipeline_job.py --no-hot weather    # archive only
    ./submit.sh pipeline_job.py --once                 # process what is in Kafka now, then stop

One application instead of one per source keeps the memory use within an 8 GB Docker VM.
"""
import argparse

from travelradar import hot, sinks
from travelradar.session import get_spark
from travelradar.sources import ALL

HOT_KINDS = {"opensky": "planes", "ais": "ships"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("sources", nargs="*", metavar="SOURCE",
                    help=f"sources to run (default: all of {', '.join(ALL)})")
    ap.add_argument("--no-hot", action="store_true", help="do not write Redis snapshots")
    ap.add_argument("--no-archive", action="store_true", help="do not write HDFS")
    ap.add_argument("--once", action="store_true",
                    help="process everything currently in Kafka, then stop (availableNow)")
    args = ap.parse_args()
    names = args.sources or list(ALL)
    unknown = sorted(set(names) - set(ALL))
    if unknown:
        ap.error(f"unknown sources {unknown}; choose from {list(ALL)}")

    spark = get_spark("TravelRadarPipeline")
    trigger = "availableNow" if args.once else None
    queries = []
    for name in names:
        source = ALL[name]
        if not args.no_archive:
            queries.append(sinks.start_raw_archive(spark, source, trigger))
            queries.append(sinks.start_cleaned_archive(spark, source, trigger))
        if not args.no_hot and name in HOT_KINDS:
            queries.append(hot.start_hot(spark, HOT_KINDS[name], trigger))

    for q in queries:
        print(f"started {q.name} ({q.id})", flush=True)
    if args.once:
        for q in queries:
            q.awaitTermination()
            print(f"finished {q.name}: {q.lastProgress and q.lastProgress['numInputRows']} rows "
                  f"in the last batch", flush=True)
        spark.stop()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
