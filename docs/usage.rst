Usage
=====

.. program:: pimdb

Downloading datasets
--------------------

To download the current IMDb datsets to the current folder, run:

.. code-block:: bash

    pimdb download all

(This downloads about 1 GB of data and might take a couple of minutes).


Transferring datasets into tables
---------------------------------

To import them in a local SQLite database :file:`pimdb.db` located in the current
folder, run:

.. code-block:: bash

    pimdb transfer all


(This will take a while. On a reasonably modern laptop with a local database
you can expect about 2 hours).

The resulting database contains one tables for each dataset. The table names
are PascalCase variants of the dataset name. For example, the date from the
dataset ``title.basics`` are stored in the table ``TitleBasics``. The column names
in the table match the names from the datasets, for example
``TitleBasics.primaryTitle``. A short description of all the datasets and
columns can be found at the download page for the
`IMDb datasets <https://www.imdb.com/interfaces/>`_.


Querying tables
---------------

To query the tables, you can use any database tool that supports SQLite, for
example the freely available and platform independent community edition of
`DBeaver <https://dbeaver.io/>`_ or the
`command line shell for SQLite <https://sqlite.org/cli.html>`_.

For simple queries you can also use :command:`pimdb`'s built-in ``query``
command, for example:

.. code-block:: bash

    pimdb query "select count(1) from TitleBasics"

The result is shown on the standard output and can be redirected to a file,
for example:

.. code-block:: bash

    pimdb query "select primaryTitle, startYear from TitleBasics limit 10" >some.tsv

You can also store a query in a text file and specify the path:

.. code-block:: bash

    pimdb query --file some-select.sql >some.tsv


Databases other than SQLite
---------------------------

.. option:: --database DATABASE

Optionally you can specify a different database using the :option:`--database` option
with an
`SQLAlchemy engine configuration <https://docs.sqlalchemy.org/en/13/core/engines.html>`_,
which generally uses the template
"dialect+driver://username:password@host:port/database". SQLAlchemy supports
several SQL `dialects <https://docs.sqlalchemy.org/en/13/dialects/index.html>`_
out of the box, and there are external dialects available for other
SQL databases and other forms of tabular data.

Here's an example for using a `PostgreSQL <https://www.postgresql.org/>`_
database:

.. code-block:: bash

    pimdb transfer --database "postgresql://user:password@localhost:5432/mydatabase" all



Building normalized tables
--------------------------

The tables so far are almost verbatim copies of the IMDb datasets with the
exception that possible duplicate rows have been removed. This means that
``NameBasics.nconst`` and ``TitleBasics.tconst`` are unique, which sadly is not
always (but still sometimes) the case for the datasets in the :file:`.tsv.gz` files.

This data model already allows to perform several kinds of queries quite
easily and efficiently.

However, the IMDb datasets do not offer a simple way to query N:M relations.
For example, the column ``NameBasics.knownForTitles`` contains a comma separated
list of tconsts like "tt2076794,tt0116514,tt0118577,tt0086491".

To perform such queries efficiently you can build strictly normalized tables
derived from the dataset tables by running:

.. code-block:: bash

    pimdb build


If you did specify a :option:`--database` for the ``transfer`` command before, you have to
specify the same value for ``build`` in order to find the source data. These tables
generally use snake_case names for both tables and columns, for example
``title_allias.is_original``.


Querying normalized tables
--------------------------

N:M relations are stored in tables using the naming template ``some_to_other``,
for example ``name_to_known_for_title``. These relation tables contain only the
numeric ID's to the respective actual data and a numeric column ``ordering`` to
remember the sort order of the comma separated list in the IMDb dataset column.

For example, here is an SQL query to list the titles Alan Smithee is known
for:

.. code-block:: sql

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

To list all movies and actors that played a character named "James Bond":

.. literalinclude:: examples/titles_with_a_jamed_bond_character.sql
    :language: sql
    :lines: 2-
    :caption: Movies with a character named "James Bond" and the respective actor


Reference
---------

To get an overview of general command line options and available commands run:

.. code-block:: bash

    pimdb --help


To learn the available command line options for a specific command run for
example:

.. code-block:: bash

    pimdb transfer --help
