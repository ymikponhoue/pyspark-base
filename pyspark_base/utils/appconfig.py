from configparser import ConfigParser


class Appconfig:

    def __init__(self, app_conf):
        self.conf_read = ConfigParser()
        self.conf_read.read(app_conf)

    def _get_app_config_value(self, section, tag):
        return self.conf_read.get(section, tag).strip()

    def get_application_name(self):
        return self._get_app_config_value("SPARK", "ApplicationName")

    def get_master(self):
        return self._get_app_config_value("SPARK", "Master")
