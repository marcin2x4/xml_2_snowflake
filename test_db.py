from db import UseDB, CredentialsError, ConnectionError
import unittest

class TestDB(unittest.TestCase):

    def test_credentials_error(self):
        with self.assertRaises(CredentialsError):
            with UseDB() as cursor:
                cursor.execute("SELECT current_version()").fetchone()

    def test_connection_error(self):
        with self.assertRaises(ConnectionError):
            with UseDB() as cursor:
                cursor.execute("SELECT current_version()").fetchone()

if __name__== '__main__':
    unittest.main()
