import yaml


class YamlReader:
    def __init__(self, file_path):
        self.file_path = file_path

    def __open_stream_file(self):
        return open(self.file_path, "r")

    def __close_stream_file(self, file_stream) -> None:
        file_stream.close()

    def parse_file_into_dict(self) -> dict:
        file_stream = self.__open_stream_file()

        py_object = yaml.safe_load(file_stream)

        self.__close_stream_file(file_stream)
        return py_object
