# GTFS Daily Analytics Project

## Overview

This project analyzes daily public transport operations in Malaysia using the GTFS dataset provided by Prasarana through the [Malaysia Open Data API](https://developer.data.gov.my/realtime-api/gtfs-static#prasarana). The goal is to provide daily insights about transport activity, including routes, trips, and stops.

The project covers:

* Daily ingestion of GTFS data
* Storage of raw data for persistent storage
* Analytical processing with DuckDB

---

## Key Features

* Downloads GTFS ZIP files from the API daily
* Extracts text files and stores them in a date-partitioned raw data directory
* Loads GTFS files into DuckDB tables
* Aggregates and joins GTFS tables to produce analytical summaries


## Notes

* Data validation is minimal since GTFS data is produced by an authoritative source.