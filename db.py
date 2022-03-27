from tempfile import tempdir
import snowflake.connector
from snowflake.connector import errors as sce
from snowflake.connector.pandas_tools import write_pandas


class ConnectionError(Exception):
    pass


class CredentialsError(Exception):
    pass


class SQLError(Exception):
    pass


# context manager
class UseDB:
    def __init__(self) -> None:
        self.user = "szpotma"
        self.password = ":Alga89666:"
        self.account = "yn90458.europe-west4.gcp"
        self.database = "DEMO_DB"
        self.schema = "PUBLIC"

    def __enter__(self) -> "cursor":
        try:
            self.conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
            )
            self.cursor = self.conn.cursor()
            return self.cursor
        except snowflake.connector.errors.InterfaceError as err:
            raise ConnectionError(err)
        except snowflake.connector.errors.ProgrammingError as err:
            raise CredentialsError(err)
        # except:
        #     print("error of other type occured in class useDB, please debug")
        except Exception as ex:
            template = "An exception of type {0} occured in class useDB. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)

    def __exit__(self, exc_type, exc_value, exc_trace):
        query_id = self.cursor.sfqid
        print(f"Query {query_id}: {self.conn.get_query_status(query_id)}")

        self.conn.commit()
        self.cursor.close()
        self.conn.close()

        # while self.conn.is_still_running(self.conn.get_query_status(query_id)):
        #     time.sleep(1)
        #     self.cursor.close()
        #     self.conn.close()

        if exc_type is snowflake.connector.errors.ProgrammingError:
            raise SQLError(exc_value)
        elif exc_type:
            raise exc_type(exc_value)

        return query_id

    def extra_attrs(**kwargs):
        def decorate(f):
            for k in kwargs:
                setattr(f, k, kwargs[k])
                print(f"ELT's {k}: {kwargs[k]}")
            return f
        return decorate

    @extra_attrs(version="1.0", process_name="REJESTR_APTEK")
    def frame_writer(self, data_frame, table_name, bool):
        try:
            self.conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
            )

            write_pandas(self.conn, data_frame, table_name, quote_identifiers=bool)

            self.conn.commit()
            self.conn.close()
            print(f"load of {table_name} completed")

        except snowflake.connector.errors.InterfaceError as err:
            raise ConnectionError(err)
        except snowflake.connector.errors.ProgrammingError as err:
            raise CredentialsError(err)
        except:
            print(
                "error of other type occured in class useDB.frame_writer(), please debug"
            )
