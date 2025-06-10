# Changes

## Version 0.3.1, 2025-06-10

This is a plain maintenance update to stop the constant tool breakage when I revisit this project after a year or so.

- Change package management to uv (issue
  [#50](https://github.com/roskakori/pimdb/issues/50)).
- Change linter to ruff (issue
  [#52](https://github.com/roskakori/pimdb/issues/52)).
- Change documentation to mkdocs (issue
  [#54](https://github.com/roskakori/pimdb/issues/54)).

## Version 0.3.0, 2024-05-13

- Fix "Column length too big" errors by switching from fixed length
  `String` field to variable `Text`. The actual
  limit, however, depends on the database.
- Add support for Python 3.12
- Remove support for Python 3.7 and 3.8. Technically, 3.8 is still maintained,
  but the current requests package already requires 3.9.

## Version 0.2.3, 2020-05-02

- Fixed `ForeignKeyViolation` when building normalized temporary table
  `characters_to_character`.
- Fixed `ValueError` when no command was specified for the
  `pimdb` command line client.

## Version 0.2.2, 2020-04-26

- Fixed `AssertionError` when command line option `--bulk` was less
  than 1.
- Added NAME `normalized` as option for `pimdb transfer` to
  transfer only the datasets needed by `pimdb build`.
- Removed redundant normalized tables `title_to(director|writer)`. Use
  relation `participation.profession_id` to limit query results to certain
  professions.
- Added a documentation chapter explaining the [data model](datamodel.md) including
  example SQL queries and overview ER diagrams.
- Added automatic removal of temporary tables only needed to build the
  normalized tables.

## Version 0.2.1, 2020-04-18

- Improved performance of command `build` for PostgreSQL by changing
  bulk `insert` to `copy from`.

## Version 0.2.0, 2020-04-16

- Fixed command `build` for PostgreSQL (issue
  [#25](https://github.com/roskakori/pimdb/issues/25)).

  - Index names now have at most 63 characters under PostgreSQL. Proper limits
    should also be in place for MS SQL and Oracle but have yet to be tested.
    SQLite always worked because it has a very large limit.
  - The PostgreSQL docker container for the test run now has more shared
    memory to allow "insert ... from select ..." with millions of
    rows. Performance still has a lot of room for improvement.

- Added TV episodes (tables `TitleEpisode` resp. `episode`).
- Cleaned up logging for `transfer` and `build` to consistently log the
  time and rows per second for each table.

## Version 0.1.2, 2020-04-14

- Fixed the remaining "value to long" errors (issue
  [#14](https://github.com/roskakori/pimdb/issues/14)).
- Fixed `TypeError` when command line option `--bulk` was specified.
- Added instructions on how to test run `pimdb` on a PostgreSQL
  docker container, see [test run with PostgreSQL](#test-run-with-postgres).

## Version 0.1.1, 2020-04-13

- Fixed "value to long" for `NameBasics.knowForTitles` (issue
  [#13](https://github.com/roskakori/pimdb/issues/13)).
- Added option to omit "sqlite:///" prefix from `--database` and specify
  only the path to the database file.
- Moved documentation to [ReadTheDocs](https://pimdb.readthedocs.io/).
- Improved performance of SQL inserts by using bulk inserts consistently and
  changing loops to SQL `insert ... from select ...` (where possible).

## Version 0.1.0, 2020-04-11

- Initial public release.
