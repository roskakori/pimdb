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

To download the current IMDb datasets to the current folder, run:

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

This will take several hours, on a MacBook Pro M1 about 11 hours.

The resulting database contains one table for each dataset. The table names
are PascalCase variants of the dataset name. For example, the date from the
dataset `title.basics` are stored in the table `TitleBasics`. The column names
in the table match the names from the datasets, for example
`TitleBasics.primaryTitle`. A short description of all the datasets and
columns can be found at the download page for the
[IMDb datasets](https://www.imdb.com/interfaces/).

Optionally you can specify a different database using the `--database` option
with an
[SQLAlchemy engine configuration](https://docs.sqlalchemy.org/en/13/core/engines.html).

### Querying tables

To query the tables, you can use any database tool that supports SQLite, for
example the freely available and platform independent community edition of
[DBeaver](https://dbeaver.io/) or the
[command line shell for SQLite](https://sqlite.org/cli.html).

For simple queries you can also use `pimdb` and look at the result as
UTF-8 encoded TSV. For example, here are the details of the top 10 oldest
people alive according to IMDb:

```bash
pimdb query "select * from NameBasics where birthYear is not null and deathYear is null order by birthYear limit 10" >oldest_people_alive.tsv
```

You can also run an SQL statement stored in a file:

```bash
pimdb query --file some.sql
```

### Building normalized tables

The tables so far are almost verbatim copies of the IMDb datasets with the
exception that possible duplicate rows have been removed. This data model
already allows to perform several kinds of queries quite easily and
efficiently.

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

This will take some time, on a MacBook Pro M1 about 30 minutes.

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

For more information on which tables are available on how they are related
read the chapter about the
[pimdb data model](https://pimdb.readthedocs.io/en/latest/datamodel.html).

## Where to go from here

Pimdb's [online documentation](https://pimdb.readthedocs.io/) describes all
aspects in further detail. You might find the following chapters of particular
interest:

- [Usage](https://pimdb.readthedocs.io/en/latest/usage.html): all command line
  options explained
- [Data model](https://pimdb.readthedocs.io/en/latest/datamodel.html):
  available tables and example SQL queries
- [Contributing](https://pimdb.readthedocs.io/en/latest/contributing.html):
  obtaining the source code and building the project locally
