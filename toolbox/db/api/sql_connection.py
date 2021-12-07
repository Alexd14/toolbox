from typing import Optional

import duckdb

from toolbox.db.settings import DB_CONNECTION_STRING


class SQLConnection:
    """
    Provides a lazy connection to a duckdb database
    """

    def __init__(self, connection_string: Optional[str] = None, read_only: bool = True) -> None:
        """
        :param connection_string: the path to the duck db data base
            If not passed then will look in settings.py for the string
        :return: None
        """
        self._read_only: bool = read_only

        self._connection_string: str = self._get_connection_string(connection_string)
        self._db_connection: Optional[duckdb.DuckDBPyConnection] = None

    @staticmethod
    def _get_connection_string(connection_string: Optional[str]) -> str:
        """
        Gets the connection string for the duckdb
        defaults to the connection_string, if that's not there then it grabs from settings.py
        :param connection_string: the path to the duck db data base
        :return: connection string to duck db data base
        :raise ValueError: if the param connection_string and DB_CONNECTION_STRING are None
        """
        if connection_string is None:
            if DB_CONNECTION_STRING is None:
                raise ValueError('Must pass a connection string or set a connection string in settings.py')
            return DB_CONNECTION_STRING

        return connection_string

    def _get_db_connection(self) -> None:
        """
        sets connection to duckdb database, if connection is currently open then it will close connection
        :return: None
        """
        if self._db_connection:
            self._db_connection.close()

        self._db_connection = duckdb.connect(database=self._connection_string, read_only=self._read_only)

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        """
        :return: connection to duckdb database
        """
        if self._db_connection is None:
            self._get_db_connection()

        return self._db_connection

    @property
    def read_only(self) -> bool:
        """
        :return: Is the connection read only?
        """
        return self.read_only

    def set_read_only(self, read_only: bool) -> None:
        """
        setter for read only
        will cause oln connection to be closed and new connection to be created
        if the passed read_only != self._read_only
        :param read_only: should the database be read only?
        :return: None
        """
        if read_only != self.read_only:
            self._read_only = read_only

            if self._db_connection:
                self._db_connection.close()
                self._connection_string = None

    def close(self) -> None:
        """
        will close the sql connection
        """
        if self._db_connection:
            self._db_connection.close()
            self._db_connection = None

    def execute(self, sql: str, **kwargs) -> duckdb.DuckDBPyConnection:
        """
        wrapper for self.con.execute(sq;)
        :param sql: query to run
        :return: raw duckdb object containing the results of the query
        """
        return self.con.execute(sql, **kwargs)

    def set_threads(self, num_threads: int) -> None:
        """
        sets the amount of threads duck db should use
        :return: None
        """
        self.con.execute(f'PRAGMA threads={num_threads};')
