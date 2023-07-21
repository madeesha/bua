import yaml


class SecretManager:
    def __init__(self, sm):
        self.sm = sm

    def fetch_secret(self, name):
        response = self.sm.get_secret_value(SecretId=name)
        return yaml.load(response['SecretString'], yaml.Loader)
