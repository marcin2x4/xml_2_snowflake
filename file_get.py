import fnmatch
import os

class FileNotFoundError(Exception):
    pass

class FileExistsError(Exception):
    pass

class FileGet:
    def __init__(self, path_dir, file_name):
        self.path = path_dir
        self.file_name = file_name

    def get_source_file_path(self):
        source_file = []

        for file_name in os.listdir(self.path):
            if fnmatch.fnmatch(file_name, self.file_name):
                source_file.append(file_name)
        try:
            source_file_name = str(source_file[0])
        except IndexError:
            raise FileNotFoundError(f"file {self.file_name} not found")

        file_dir = self.path
        file_full_path = os.path.join(self.path, source_file_name)

        return file_full_path, source_file_name, file_dir

    @staticmethod
    def file_rename(file, dir):
        source_file_new_name = "old_" + file

        if os.path.isfile(dir + source_file_new_name):
            raise FileExistsError("this file was already processed!")
        else:
            os.rename(os.path.join(dir, file), os.path.join(dir, source_file_new_name))

