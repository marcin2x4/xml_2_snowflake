from file_get import FileGet
import unittest
import os

class TestFileGet(unittest.TestCase):

    def test_file_get(self):
        file_path = FileGet("/home/marcin2x4/projects/python/xml_2_snf", "function.py")
        self.assertIs(type(file_path.path), str)
        self.assertIs(type(file_path.file_name), str)

    def test_source_file_path_exists(self):
        self.assertTrue(os.path.exists("/home/marcin2x4/projects/python/xml_2_snf"))

if __name__== '__main__':
    unittest.main()
    