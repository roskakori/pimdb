Contributing
============

Project setup
-------------

In case you want to play with the source code or contribute changes proceed as
follows:

1. Check out the project from GitHub:

   .. code-block:: bash

       $ git clone https://github.com/roskakori/pimdb.git
       $ cd pimdb

2. Create and activate a virtual environment:

   .. code-block:: bash

       $ python -m venv venv
       $ . venv/bin/activate

3. Install the required packages:

   .. code-block:: bash

       $ pip install --upgrade pip
       $ pip install -r requirements.txt

4. Install the pre-commit hook:

   .. code-block:: bash

       $ pre-commit install


Testing
-------

To run the test suite:

.. code-block:: bash

    $ pytest

To build and browse the coverage report in HTML format:

.. code-block:: bash

    $ pytest --cov-report=html
    $ open htmlcov/index.html  # macOS only

.. envvar:: PIMDB_TEST_DATABASE

By default, all database related tests run on SQLite. Some tests can run on
different databases in order to test that everything works across a wide
range. To use a specific database, set the respective engine in the
environment variable :envvar:`PIMDB_TEST_DATABASE`. For example:

.. code-block:: bash

    export PIMDB_TEST_DATABASE="postgresql+psycopg2://postgres@localhost:5439/pimdb_test"

.. _test-run-with-postgres:

Test run with PostgreSQL
-----------------------------

While the test suite uses SQLite, you can test run :command:`pimdb` on a
PostgreSQL database in a docker container:

1. Install `Docker Desktop <https://www.docker.com/get-started>`_
2. Run the postgres container in port 5439 (possibly using :command:`sudo`):

   .. code-block:: bash

        docker-compose --file tests/docker-compose.yml up postgres

3. Create the database (possibly using :command:`sudo`):

   .. code-block:: bash

        docker exec -it pimdb_postgres psql --username postgres --command "create database pimdb"

   If you want a separate database for the unit tests:

        docker exec -it pimdb_postgres psql --username postgres --command "create database pimdb_test"

4. Run :command:`pimdb`:

   .. code-block:: bash

        pimdb transfer --dataset-folder tests/data --database postgresql+psycopg2://postgres@localhost:5439/pimdb all


Documentation
-------------

To build the documentation in HTML format:

.. code-block:: bash

    $ make -C docs html
    $ open docs/_build/html/index.html  # macOS only


Coding guidelines
-----------------

The code throughout uses a natural naming schema avoiding abbreviations, even
for local variables and parameters.

Many coding guidelines are automatically enforced (and some even fixed
automatically) by the pre-commit hook. If you want to check and clean up
the code without performing a commit, run:

.. code-block:: bash

    $ pre-commit run --all-files

In particular, this applies `black <https://black.readthedocs.io/en/stable/>`_,
`flake8 <https://flake8.pycqa.org/>`_ and
`isort <https://pypi.org/project/isort/>`_.
