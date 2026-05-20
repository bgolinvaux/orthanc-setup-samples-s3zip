# Purpose

This is a sample setup to compare the performances of DB engines.

# Description

This demo contains:

- 3 Orthanc containers, one with SQLite, one with PostgreSQL and one with MySQL.
- a small test scripts that measures performance and generates a plot.

# Starting the setup

To start the setup, type: `docker-compose up --build`.
Use `docker-compose down -v` and delete the results files (`sudo rm results/*.txt`) between runs.

One liner:

```
docker-compose down -v && sudo rm results/*.txt && docker-compose up --build
```

# results

The results are available in the Orthanc Book.
