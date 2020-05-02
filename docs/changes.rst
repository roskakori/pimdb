Changes
=======

Version 0.2.3, 2020-05-02

* Fixed :py:exc:`ForeignKeyViolation` when building normalized temporary table
  ``characters_to_character``.
* Fixed :py:exc:`ValueError` when no command was specified for the
  :command:`pimdb` command line client.

Version 0.2.2, 2020-04-26

* Fixed :py:exc:`AssertionError` when command line option ``--bulk`` was less
  than 1.
* Added NAME ``normalized`` as option for :command:`pimdb transfer` to
  transfer only the datasets needed by :command:`pimdb build`.
* Removed redundant normalized tables ``title_to(director|writer)``. Use
  relation ``praticipation.profession_id`` to limit query results to certain
  professions.
* Added documentation chapter explaining the :doc:`datamodel` including
  example SQL queries and overview ER diagrams.
* Added automatic removal of temporary tables only needed to build the
  normalized tables.

Version 0.2.1, 2020-04-18

* Improved performance of command :command:`build` for PostgreSQL by changing
  bulk ``insert`` to ``copy from``.

Version 0.2.0, 2020-04-16

* Fixed command :command:`build` for PostgreSQL (issue
  `#25 <https://github.com/roskakori/pimdb/issues/25>`_).:

  * Index names now have at most 63 characters under PostgreSQL. Proper limits
    should also be in place for MS SQL and Oracle but have yet to be tested.
    SQLite always worked because it has a very large limit.
  * The PostgreSQL docker container for the test run now has more shared
    memory in order to allow "insert ... from select ..." with millions of
    rows. Performance still has a lot of room for improvement.

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
