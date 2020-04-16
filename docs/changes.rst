Changes
=======

Version 0.2.0, 2020-04-xx

* Added TV episodes (tables ``TitleEpisode`` resp. ``episode``).
* Cleaned up logging for ``transfer`` and ``build`` to consistently log the
  time and rows per second for each table.

Version 0.1.2, 2020-04-14

* Fixed remaining "value to long" errors (issue
  `#14 <https://github.com/roskakori/pimdb/issues/14>`_).
* Fixed :py:exc:`TypeError` when command line option `--bulk` was specified.
* Added instructions on how to test run :command:`pimdb` on a PostgreSQL
  docker container, see :ref:`test-run-with-postgres`.

Version 0.1.1, 2020-04-13

* Fixed "value to long" for ``NameBasics.knowForTitles`` (issue
  `#13 <https://github.com/roskakori/pimdb/issues/13>`_).
* Added option to omit "sqlite:///" prefix from ``--database`` and specify
  only the path to the database file.
* Moved documentation to `ReadTheDocs <https://pimdb.readthedocs.io/>`_.
* Improved performance of SQL inserts by using bulk inserts consistently and
  changing loops to SQL ``insert ... from select ...``  (where possible).

Version 0.1.0, 2020-04-11

* Initial public release.
