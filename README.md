# DP_practice: a Spark ETL pipeline in Docker Compose

Course project for Data Processing (U-tad, 2025-26) by Itziar Morales Rodríguez.

It builds a small data platform with Docker Compose, loads the [OpenFlights](https://openflights.org/data) dataset into three different stores with Spark (Scala), and then queries all of it from Spark SQL as permanent tables.

The full story, including the bugs I hit along the way, is on my site: [Spark, HDFS, and Two Databases](https://itziar.dev/notes/flight-data-pipeline/) :)

## Architecture

```
OpenFlights files (docker/data → /data)
        │
        ▼
Spark Standalone cluster (1 master + 2 workers), Scala in spark-shell
   ├── countries ──► HDFS (CSV)
   ├── airlines  ──► HDFS (Parquet)
   ├── airports  ──► PostgreSQL (JDBC)
   └── routes    ──► MongoDB (enriched with source and destination country,
                              active airlines only)
        │
        ▼
Spark SQL: the four datasets registered as permanent tables in the
`practice` database → analysis queries → aggregation written back to HDFS (Parquet)
```

## Stack

| Piece | Version / detail |
|---|---|
| Apache Spark | 3.5.8 (Scala 2.12), Standalone: 1 master, 2 workers |
| HDFS | Custom image in `docker/hdfs/` |
| PostgreSQL | 9.6, through the JDBC driver in `docker/jars/` (42.7.3) |
| MongoDB | 6.0, through the MongoDB Spark connector |
| Orchestration | Docker Compose |

## What each script does

| Script | Reads | Writes | Notes |
|---|---|---|---|
| `code/2_1_ingestion_countries.scala` | `countries.txt` | HDFS, CSV | Custom `key::value ## …` line format, parsed into a typed Dataset |
| `code/2_2_ingestion_airlines.scala` | `airlines.dat` | HDFS, Parquet | |
| `code/2_3_ingestion_airports.scala` | `airports.dat` | PostgreSQL, table `airports` | Read back with a partitioned JDBC read (4 partitions on `airport_id`) |
| `code/2_4_ingestion_routes.scala` | `routes.dat` | MongoDB | Joined with airports to add source and destination country; inner join keeps active airlines only |
| `code/3_analysis.scala` | All four stores | Spark SQL tables, HDFS (Parquet) | Six questions, each answered twice: DataFrame API and SQL |

The six questions:

1. How many flights depart from Brazil?
2. Which airports are in the north-west quadrant?
3. Which countries have the most airports above 2,000 feet?
4. What are the top 10 routes with the most stops?
5. What is the average number of routes per airline?
6. What are the ISO codes of the countries with routes that have an empty codeshare?

The final aggregation (airports per country with at least two routes to airports in DST region `O`) is written to HDFS as Parquet.

## Things I learnt

- **`saveAsTable` on a multi-container cluster needs an explicit HDFS `path`.** Without it, the warehouse resolves to the local disk of whichever worker runs the write, so the tables come out empty and Spark throws no error. `DESCRIBE FORMATTED` showed the location was `file:/opt/spark/spark-warehouse/...`.
- **Check the data, not just the code.** `countries.txt` has a real duplicate row for India (same name and ISO code, different DAFIF code). The DataFrame version of Q6 returned 208 rows and the SQL version 207 until I added a second `.distinct()` after the join.
- **Read the field definitions.** OpenFlights has a numeric UTC offset (`timezone`) and a one-letter DST region (`dst`). "Timezone 'O'" only makes sense for `dst`.

## Running it

```bash
# 1. Put the OpenFlights files (countries.txt, airlines.dat, airports.dat, routes.dat)
#    in docker/data/ (gitignored; mounted at /data in the containers)

# 2. Start the platform
cd docker
docker compose up -d
```

Web UIs once it's up:

- Spark master: http://localhost:8080
- Spark application (while a shell is running): http://localhost:4040
- HDFS NameNode: http://localhost:9870

Then open a `spark-shell` on the master (`spark://spark-master:7077`) with the Postgres JDBC jar and the MongoDB Spark connector available, and run the scripts in order: `2_1` → `2_2` → `2_3` → `2_4` → `3_analysis`.

## Repository layout

```
DP_practice/
├── code/        # Scala scripts, run in spark-shell
├── docker/      # docker-compose files, HDFS image, Spark config, JDBC jar
│   └── data/    # OpenFlights files (gitignored)
└── report/      # LaTeX report with screenshots of every step (main.pdf)
```

## Report

`report/main.pdf` walks through every step with screenshots: setting up each container, starting the cluster, each ingestion and each query. The LaTeX source is in `report/`.
