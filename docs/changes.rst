Changes
=======

Version 0.2.0, 2020-04-13

* Added option to omit "sqlite:///" prefix from :option:`--database` and
  specify only the path to the database file.
* Moved documentation to `ReadTheDocs <https://pimdb.readthedocs.io/>`_.
* Improved performance of SQL inserts by using bulk inserts consistently and
  changing loops to SQL ``insert ... from select ...``  (where possible).

Version 0.1.0, 2020-04-11

* Initial public release.
