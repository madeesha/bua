from typing import Dict

import yaml


class SecretsManagerClientStub:

    def __init__(self, secrets: Dict):
        self.expected_get_secret_keys = {'SecretId'}
        self.expected_secrets = secrets

    def get_secret_value(self, **kwargs):
        for k in kwargs.keys():
            assert k in self.expected_get_secret_keys
        return {
            'SecretString': yaml.dump(self.expected_secrets[kwargs['SecretId']])
        }
