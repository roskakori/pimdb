Changes
=======

Version 0.2.0, 2020-04-13

* Fixed "value to long" for ``NameBasics.knowForTitles`` (issue
  `#13 <https://github.com/roskakori/pimdb/issues/13>`_).
* Added option to omit "sqlite:///" prefix from ``--database`` and specify
  only the path to the database file.
* Moved documentation to `ReadTheDocs <https://pimdb.readthedocs.io/>`_.
* Improved performance of SQL inserts by using bulk inserts consistently and
  changing loops to SQL ``insert ... from select ...``  (where possible).

Version 0.1.0, 2020-04-11

* Initial public release.
