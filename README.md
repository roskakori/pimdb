# pimdb

Pimdb is a python package and command line utility to maintain a local copy of
the essential parts of the
[Internet Movie Database](https://imdb.com) (IMDb) based in the TSV files
available from [IMDb datasets](https://www.imdb.com/interfaces/).


## License

The [IMDb datasets](https://www.imdb.com/interfaces/) are only available for
personal and non-commercial use. For details refer to the previous link.

Pimdb is open source and distributed under the
[BSD license](https://opensource.org/licenses/BSD-3-Clause). The source
code is available from https://github.com/roskakori/pimdb.


## Installation

Pimdb is available from [PyPI](https://pypi.org/project/pimdb/) and can be
installed using:

```bash
$ pip install pimdb
```


## Quick start


### Downloading datasets

To download the current IMDb datsets to the current folder, run:

```bash
pimdb download all
```

(This downloads about 1 GB of data and might take a couple of minutes).


### Transferring datasets into tables

To import them in a local SQLite database `pimdb.db` located in the current
folder, run:

```bash
pimdb transfer all
```

(This will take a while. On a reasonably modern laptop with a local database
you can expect about 2 hours).

The resulting database contains one tables for each dataset. The table names
are PascalCase variants of the dataset name. For example, the date from the
dataset `title.basics` are stored in the table `TitleBasics`. The column names
in the table match the names from the datasets, for example
`TitleBasics.primaryTitle`. A short description of all the datasets and
columns can be found at the download page for the
[IMDb datasets](https://www.imdb.com/interfaces/).


### Querying tables

To query the tables, you can use any database tool that supports SQLite, for
example the freely available and platform independent community edition of
[DBeaver](https://dbeaver.io/) or the
[command line shell for SQLite](https://sqlite.org/cli.html).


### Databases other than SQLite

Optionally you can specify a different database using the `--database` option
with an
[SQLAlchemy engine configuration](https://docs.sqlalchemy.org/en/13/core/engines.html),
which generally uses the template
"dialect+driver://username:password@host:port/database". SQLAlchemy supports
several SQL [dialects](https://docs.sqlalchemy.org/en/13/dialects/index.html)
out of the box, and there are external dialects available for other
SQL databases and other forms of tabular data such as
[pydruid](https://github.com/druid-io/pydruid) (for Pandas),
[PyHive](https://github.com/dropbox/PyHive#sqlalchemy) (for Presto and Hive)
or [Solr](https://github.com/aadel/sqlalchemy-solr) (for the Solr search
platform).

Here's an example for using a [PostgreSQL](https://www.postgresql.org/)
database:

```bash
pimdb transfer --database "postgresql://user:password@localhost:5432/mydatabase" all
```


### Building normalized tables

The tables so far are almost verbatim copies of the IMDb datasets with the
exception that possible duplicate rows have been removed. This means that
`NameBasics.nconst` and `TitleBasics.tconst` are unique, which sadly is not
always (but still sometimes) the case for the datasets in the `.tsv.gz` files.

This data model already allows to perform several kinds of queries quite
easily and efficiently.

However, the IMDb datasets do not offer a simple way to query N:M relations.
For example, the column `NameBasics.knownForTitles` contains a comma separated
list of tconsts like "tt2076794,tt0116514,tt0118577,tt0086491".

To perform such queries efficiently you can build strictly normalized tables
derived from the dataset tables by running:

```bash
pimdb build
```
If you did specify a `--database` for the `transfer` command before, you have to
specify the same value for `build` in order to find the source data. These tables
generally use snake_case names for both tables and columns, for example
`title_allias.is_original`.


## Querying normalized tables

N:M relations are stored in tables using the naming template `some_to_other`,
for example `name_to_known_for_title`. These relation tables contain only the
numeric ID's to the respective actual data and a numeric column `ordering` to
remember the sort order of the comma separated list in the IMDb dataset column.

For example, here is an SQL query to list the titles Alan Smithee is known
for:

```sql
select
    title.primary_title,
    title.start_year
from
    name_to_known_for_title
    join name on
        name.id = name_to_known_for_title.name_id
    join title on
        title.id = name_to_known_for_title.title_id
where
    name.primary_name = 'Alan Smithee'
```


## Reference

To get an overview of general command line option and available commands run:

```bash
pimdb --help
```

To learn the available command line options for a specific command run for
example:

```bash
pimdb transfer --help
```


## Changes

Version 0.1.0, 2020-04-11

* Initial public release.
