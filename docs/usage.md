# Usage

## Downloading datasets

To download the current IMDb datsets to the current folder, run:

```bash
pimdb download all
```

(This downloads about 1 GB of data and might take a couple of minutes).

## Transferring datasets into tables

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

## Querying tables

To query the tables, you can use any database tool that supports SQLite, for
example the freely available and platform independent community edition of
[DBeaver](https://dbeaver.io/) or the
[command line shell for SQLite](https://sqlite.org/cli.html).

For simple queries you can also use `pimdb`'s built-in `query`
command, for example:

```bash
pimdb query "select count(1) from TitleBasics"
```

The result is shown on the standard output and can be redirected to a file,
for example:

```bash
pimdb query "select primaryTitle, startYear from TitleBasics limit 10" >some.tsv
```

You can also store a query in a text file and specify the path:

```bash
pimdb query --file some-select.sql >some.tsv
```

## Databases other than SQLite

Optionally you can specify a different database using the `--database` option
with an
[SQLAlchemy engine configuration](https://docs.sqlalchemy.org/en/13/core/engines.html),
which generally uses the template
"dialect+driver://username:password@host:port/database". SQLAlchemy supports
several SQL [dialects](https://docs.sqlalchemy.org/en/13/dialects/index.html)
out of the box, and there are external dialects available for other
SQL databases and other forms of tabular data.

Here's an example for using a [PostgreSQL](https://www.postgresql.org/)
database:

```bash
pimdb transfer --database "postgresql://user:password@localhost:5432/mydatabase" all
```

## Building normalized tables

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

To list all movies and actors that played a character named "James Bond":

```sql
-- Movies with a character named "James Bond" and the respective actor
select
    title.primary_title as movie,
    title.start_year as year,
    name.primary_name as actor
from
    title_character
    join title on
        title.id = title_character.title_id
    join name_to_title_character on
        name_to_title_character.title_character_id = title_character.id
    join name on
        name.id = name_to_title_character.name_id
where
    title_character.name = 'James Bond'
order by
    title.start_year,
    title.primary_title
```

## Reference

To get an overview of general command line options and available commands run:

```bash
pimdb --help
```

To learn the available command line options for a specific command run for
example:

```bash
pimdb transfer --help
```
