"""Database bulk operations."""

# Copyright (c) 2020, Thomas Aglassinger.
# All rights reserved. Distributed under the BSD License.
from typing import IO, Any, Optional

from sqlalchemy import Table
from sqlalchemy.engine import Connection, Engine

from pimdb.common import log

#: Default number of bulk data (for e.g. SQL insert) to be collected in memory before they are sent to the database.
DEFAULT_BULK_SIZE = 1024


class BulkError(Exception):
    """
    Error indicating that something went wrong during a bulk operation.
    """


class BulkInsert:
    """
    Database insert in bulks. While the interface allows rows to be inserted
    one by one using :py:meth:`add`, the are collected in a list until it
    contains ``bulk_size`` rows and only then flushed to the database. This
    improves performance by reducing the number of interactions with the
    database API while making it simple to not exceed the maximum size of an
    ``insert values`` SQL statement the database can handle.
    """

    def __init__(self, connection: Connection, table: Table, bulk_size: int = DEFAULT_BULK_SIZE):
        assert bulk_size >= 1
        self._connection = connection
        self._table = table
        self._bulk_size = bulk_size
        self._data = []
        self._count = 0

    def add(self, data: dict[str, Optional[Any]]):
        self._data.append(data)
        self._count += 1
        if len(self._data) >= self._bulk_size:
            self._flush()

    def _flush(self):
        data_count = len(self._data)
        assert data_count >= 1
        log.debug("    inserting %d data to %s", data_count, self._table.name)
        insert = self._table.insert(self._data)
        self._connection.execute(insert)
        self._data.clear()

    @property
    def count(self):
        """
        Number of rows collected to far. Not all of them might have been sent
        to the database yet.
        """
        return self._count

    def close(self):
        if len(self._data) >= 1:
            self._flush()
        self._data = None

    def __enter__(self):
        return self

    def __exit__(self, error_type, error_value, error_traceback):
        if not error_type:
            self.close()


class PostgresBulkLoad:
    def __init__(self, engine: Engine):
        self._engine = engine

    def load(self, target_table: Table, source: IO, append: bool = False):
        raw_connection = self._engine.raw_connection()
        try:
            with raw_connection.cursor() as cursor:
                # NOTE: Some text fields do start with double quotes but do
                #  not end with it before the next tab delimiter, so with
                #  the defaults PostgreSQL's "copy from" would believe this is
                #  a very long field. To prevent this from happening we use an
                #  escape and quote character that are unlikely to show up in
                #  the TSV.
                #
                #  If would have been even nice to use characters that would
                #  be impossible. For UTF-8 streams this can easily be
                #  achieved by having more than 4 of the initial bits set to
                #  1 (see https://en.wikipedia.org/wiki/UTF-8), for example:
                #
                #  escape_character = chr(0b11111100)
                #  quote_character = chr(0b11111101)
                #
                #  However "copy" rejects this because it seems to allow
                #  only ASCII characters as escape and quote characters.
                escape_character = "\f"
                quote_character = "\v"
                if not append:
                    cursor.execute(f'truncate "{target_table.name}"')
                command = (
                    f'copy "{target_table.name}" from stdin with ('
                    f"delimiter '\t', encoding 'utf-8', escape '{escape_character}', "
                    f"format csv, header, null '\\N', quote '{quote_character}')"
                )
                log.debug("  performing: %r", command)
                cursor.copy_expert(command, source)
            raw_connection.commit()
        finally:
            raw_connection.close()

    def close(self):
        # For now, do nothing.
        pass

    def __enter__(self):
        return self

    def __exit__(self, error_type, error_value, error_traceback):
        if not error_type:
            self.close()
