# Data Connector Agent for SQLite

This directory contains an SQLite implementation of a data connector agent.

## Requirements

* NodeJS 16
* SQLite `>= 3.38.0` or compiled in JSON support
    * Required for the json_group_array() and json_group_object() aggregate SQL functions
    * https://www.sqlite.org/json1.html#jgrouparray

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
> docker build . -t dc-sqlite-agent:latest
> docker run -it --rm -p 8100:8100 dc-sqlite-agent:latest
```

# Dataset

The dataset used for testing the reference agent is sourced from https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sql


# TODO

* [ ] Ensure everything is escaped correctly
* [ ] Or... Use parameterized queries if possible
* [ ] Run test-suite from SDK
* [x] Remove old queries module
* [x] Relationships / Joins
* [ ] Rename `resultTT` and other badly named types in the `schema.ts` module
* [ ] Add ENV Variable for restriction on what databases can be used
* [ ] Update to the latest types
* [ ] Port back to hge codebase as an official reference agent
* [x] Make escapeSQL global to the query module
* [ ] Look for logs of `Couldn't find relationship for`
