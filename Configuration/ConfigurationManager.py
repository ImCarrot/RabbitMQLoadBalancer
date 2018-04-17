import pkg_resources
import json


class ConfigurationManager(object):
    __instance = None

    def __new__(cls, *args, **kwargs):
        if ConfigurationManager.__instance is None:
            ConfigurationManager.__instance = object.__new__(cls)
        return ConfigurationManager.__instance

    def __init__(self):
        self._config_contents = self.read_config()

    @staticmethod
    def read_config()->dict:
        """
        reads the config json file
        :return: the contents of the config file
        """
        try:
            path_components = __name__.split('.')[:-1]
            resource_path = '.'.join(path_components)
            cached_file = pkg_resources.resource_filename(resource_path, 'app.prop')

            # check if the file exists at the path.
            file_contents = json.load(open(cached_file))

            # clean up all resources
            pkg_resources.cleanup_resources()

            # return the file contents
            return file_contents

        except FileNotFoundError:
            print('Could not locate the "app.prop". '
                                  'Please make sure it exists in the same directory.')

        except json.JSONDecodeError:
            print("The configuration file must have a valid JSON.")

        except TypeError:
            print("The configuration file must have a valid JSON.")

    def get_value(self, key: str):
        """
        Gets the value for a particular key in the config
        :param key: the key of the value that needs to be fetched
        :return:
        """
        try:
            return self._config_contents[key]
        except KeyError:
            print(f"Could not find the desired key: {key} in the config file")

    def get_keys(self)->list:
        """
        Gets a list of all the keys in the config file
        :return: a list of all keys from the config file
        """
        return list(self._config_contents.keys())
