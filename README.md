# Data Connector Agent for SQLite

This directory contains an SQLite implementation of a data connector agent.

## Requirements

* NodeJS 16

## Build & Run

```
> npm install
> npm start
```

## Agent usage

The agent is configured as per the configuration schema.

The only required field is `db` which specifies a local sqlite database to use.

The schema is exposed via introspection, but you can limit which tables are referenced by

* Explicitly enumerating them via the `tables` field, or
* Toggling the `include_sqlite_meta_tables` to include or exclude sqlite meta tables.


## Docker Build & Run

```
> docker build . -t dc-reference-agent:latest
> docker run -it --rm -p 8100:8100 dc-reference-agent:latest
```

# Dataset

The test dataset exposed by the reference agent is sourced from https://www.sqlitetutorial.net/sqlite-sample-database/

