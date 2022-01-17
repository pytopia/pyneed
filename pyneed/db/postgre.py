from typing import Union, List
import psycopg2
import psycopg2.extras
from collections.abc import Iterable
import time


LIMIT_RETRIES = 5

class PgManager:

    def __init__(self):
        self._connection = None
        self._cursor = None
        self.reconnect = True
        self.connect()
        self.cursor()

    def connect(self, retry_counter=0):
        if self._connection:
            return

        try:
            print("Connecting to DB")
            self._connection = psycopg2.connect(
                user=self.affinity_db_config.user,
                host=self.affinity_db_config.host,
                port=self.affinity_db_config.port,
                database=self.affinity_db_config.dbname,
                sslmode="require",
                connect_timeout=30
            )
            retry_counter = 0
            self._connection.autocommit = False
            return self._connection
        except psycopg2.OperationalError as error:
            if not self.reconnect or retry_counter >= LIMIT_RETRIES:
                raise error
            else:
                retry_counter += 1
                print("got error {}. reconnecting {}".format(
                    str(error).strip(), retry_counter))
                time.sleep(5)
                self.connect(retry_counter)
        except (Exception, psycopg2.Error) as error:
            raise error

    def cursor(self):
        if not self._cursor or self._cursor.closed:
            if not self._connection:
                self.connect()
            self._cursor = self._connection.cursor(
                cursor_factory=psycopg2.extras.NamedTupleCursor)
            return self._cursor

    def execute(self, query, retry_counter=0, is_one=False):
        try:
            #query = "set statement_timeout=3;" + query
            self._cursor.execute(query)
            retry_counter = 0
        except (psycopg2.DatabaseError, psycopg2.OperationalError) as error:
            if retry_counter >= LIMIT_RETRIES:
                raise error
            else:
                retry_counter += 1
                print(f"got error {str(error).strip()}. retrying {retry_counter}")
                time.sleep(1)
                self.reset()
                self.execute(query, retry_counter)
        except (Exception, psycopg2.Error) as error:
            raise error
        if is_one:
            return self._cursor.fetchone()
        else:
            return self._cursor.fetchall()

    def reset(self):
        self.close()
        self.connect()
        self.cursor()

    def close(self):
        if self._connection:
            if self._cursor:
                self._cursor.close()
            self._connection.close()
            print("PostgreSQL connection is closed")
        self._connection = None
        self._cursor = None

    def select(
        self,
        table: str,
        *,
        columns: Union[List, str] = '*',
        filters: dict = {},
        order_by=None,
        ascending=True,
        limit: int = 1,
        random: bool = False
    ) -> List:
        """Runs a elect query: SELECT columns FROM table LIMIT limit ORDER BY random;
        Usage example:
        >>> cur.select(mytable, limit=10)
        runs: SELECT * FROM mytable LIMIT 10;
        >>> cur.select(mytable, ['column_1', 'column_2'], limit=10, random=True)
        runs: SELECT column_1, column_2 FROM mytable LIMIT 10 ORDER BY random();
        >>> cur.select('mytable', filters={'is': {'title': None}, '=': {'owner': 'affinity'}})
        runs: SELECT * FROM mytable WHERE title is null AND owner = 'affinity';
        :param table: Table to query on.
        :param columns: Columns you want to retrieve, defaults to '*' retrieving all columns
        :param filters: Filters to apply in retrieval.
        :param limit: Limit number of rows to return, defaults to 1
        :param random: Random sampling flag, defaults to False
        :return: List of rows returned by query.
        """
        # columns to retrieve, "*" means all columns
        if type(columns) is list:
            columns = ','.join(columns)

        # query
        query = f"SELECT {columns} FROM {table}"

        # add filters
        all_filters = []
        query_filters = ""
        for filter_type, filter_conditions in filters.items():

            # collect all filters
            for column, value in filter_conditions.items():

                # if value is string, it is converted to 'value'
                # otherwise it stays without quotations
                # like when value is None it stays as None not 'None'
                # or when value is 1234, it is not converted to '1234'
                if type(value) is str:
                    value = f"'{value}'"

                elif value is None:
                    value = 'null'

                # if value is iterable, convert it to (item_1, item_2, ...)
                # this is for IN query (match against a list of values)
                elif isinstance(value, Iterable):
                    value = f"({','.join(map(str, value))})"

                all_filters.append(f"{column} {filter_type} {value}")

            # AND all filters
            query_filters = " AND ".join(all_filters)

        if query_filters:
            query = f"{query} WHERE {query_filters}"

        # add options
        query = f"{query} ORDER BY random()" if random else query
        query = f"{query} ORDER BY {order_by}" if order_by else query
        query = f"{query} DESC" if ascending is False else query
        query = f"{query} LIMIT {limit}" if limit else query

        # return data
        self.last_query = query
        return self.execute(query)

    def batch_execute(self, query, itersize=2000, retry_counter=0, is_one=False):
        with self._connection.cursor(name='server_side', cursor_factory=psycopg2.extras.NamedTupleCursor) as cursor:
            cursor.itersize = itersize
            cursor.execute(query)
            for row in cursor:
                yield row
