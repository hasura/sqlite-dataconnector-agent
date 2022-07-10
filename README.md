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

## Options / Environment Variables

* ENV: `PERMISSIVE_CORS={1|true|yes}` - Allows all requests - Useful for testing with SwaggerUI. Turn off on production.
* ENV: `DB_CREATE={1|true|yes}` - Allows new databases to be created, not permitted by default.
* ENV: `DB_READONLY={1|true|yes}` - Makes databases readonly, they are read-write by default.
* ENV: `DB_ALLOW_LIST=DB1[,DB2]*` - Restrict what databases can be connected to.
* ENV: `DB_PRIVATECACHE` - Keep caches between connections private. Shared by default.

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

You will want to mount a volume with your database(s) so that they can be referenced in configuration.

# Dataset

The dataset used for testing the reference agent is sourced from https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sql

# Testing Changes to the Agent

Run:

```sh
cabal run graphql-engine:test:tests-dc-api -- test --agent-base-url http://localhost:8100 --agent-config '{"db": "db.chinook2.sqlite"}'
```

From the HGE repo.


# TODO

* [x] Health Check
* [x] DB Specific Health Checks
* [x] Schema
* [x] Capabilities
* [x] Query
* [x] Array Relationships
* [x] Object Relationships
* [ ] Ensure everything is escaped correctly
* [ ] Or... Use parameterized queries if possible
* [x] Run test-suite from SDK
* [x] Remove old queries module
* [x] Relationships / Joins
* [ ] Rename `resultTT` and other badly named types in the `schema.ts` module
* [x] Add ENV Variable for restriction on what databases can be used
* [x] Update to the latest types
* [ ] Port back to hge codebase as an official reference agent
* [x] Make escapeSQL global to the query module
* [x] Make CORS permissions configurable
* [x] Optional DB Allowlist
* [ ] Fix SDK Test suite to be more flexible about descriptions
* [x] READONLY option
* [x] CREATE option
* [x] Don't create DB option
* [ ] Verbosity settings
* [x] Cache settings
* [x] Missing WHERE clause from object relationships

# Known Bugs

## Where criteria for related tables

Handling `WHERE: {...}` criteria for related tables

Example:

```graphql
query {
  Artist(where: { Albums: { Name { _eq: "Foo" } } })
}
```

## ORDER BY Breaks Output

Ordering clauses can cause results to be generated incorrectly - rows of strings, as opposed to rows of objects.

```graphql
query MyQuery {
  Track(limit: 4, order_by: {Name: desc}) {
    Name
    Album {
      Title
    }
  }
}
```

Example:

```sql
SELECT (
    SELECT json_group_array(j)
    FROM (
        SELECT json_object(
            'Album', ( SELECT json_object('Title', Title) as j FROM Album),
            'Name', Name
        ) as j
        FROM Track
        WHERE (Name > 'S')
        LIMIT 3
    )
) as data
```

Gives

```json
[{"Album":{"Title":"For Those About To Rock We Salute You"},"Name":"Snowballed"},{"Album":{"Title":"For Those About To Rock We Salute You"},"Name":"Spellbound"},{"Album":{"Title":"For Those About To Rock We Salute You"},"Name":"Whole Lotta Rosie"}]
```

while

```sql
SELECT  (
    SELECT json_group_array(j)
    FROM (
        SELECT json_object(
            'Album',  (SELECT json_object('Title', Title) as j FROM Album),
            'Name', Name
        ) as j
        FROM Track
        WHERE (Name > 'S')
        ORDER BY Name asc 
        LIMIT 3 
    )
)  as data
```

Gives

```json
["{\"Album\":{\"Title\":\"For Those About To Rock We Salute You\"},\"Name\":\"Sabbra Cadabra\"}","{\"Album\":{\"Title\":\"For Those About To Rock We Salute You\"},\"Name\":\"Sad But True\"}","{\"Album\":{\"Title\":\"For Those About To Rock We Salute You\"},\"Name\":\"Salgueiro\"}"]
```
