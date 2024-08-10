from db import UseDB, CredentialsError, ConnectionError
import unittest

class TestDB(unittest.TestCase):

    def test_credentials_error(self):
        with self.assertRaises(CredentialsError) as ce:
            with UseDB() as cursor:
                cursor.execute("SELECT current_version()").fetchone()
            the_exception = ce.exception

            return the_exception

if __name__== '__main__':
    unittest.main()
