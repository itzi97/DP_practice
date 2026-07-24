# DP_practice

Data Processing mandatory practice — Academic year 2025-2026.

## Stack
- Apache Spark 3.5.x Standalone
- HDFS
- PostgreSQL (JDBC)
- MongoDB (Spark connector)
- Docker

## Structure

```
DP_practice/
├── report/
│   ├── main.tex
│   └── assets/
│       ├── tex/        # LaTeX sections
│       └── imgs/       # Screenshots
├── docker/
│   ├── docker-compose.yml
│   ├── spark-defaults.conf
│   ├── hdfs/
│   └── data/           # Data files (gitignored)
├── code/
│   ├── p2_countries.scala
│   ├── p2_airlines.scala
│   ├── p2_airports.scala
│   ├── p2_routes.scala
│   └── p3_analysis.scala
├── README.md
└── .gitignore
```

## Running the environment

```bash
cd docker
docker compose up -d
```

## Compiling the report

```bash
cd report
pdflatex main.tex
```
