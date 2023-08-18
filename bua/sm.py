import yaml


class SecretManager:
    def __init__(self, sm_client):
        self.sm_client = sm_client

    def fetch_secret(self, name):
        response = self.sm_client.get_secret_value(SecretId=name)
        return yaml.load(response['SecretString'], yaml.Loader)
