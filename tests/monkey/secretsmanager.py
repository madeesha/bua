import json


class MonkeyPatchSecretsManagerClient:

    def get_secret_value(self, *args, **kwargs):
        return {
            'SecretString': json.dumps({
                'rdshost': '',
                'username': '',
                'password': '',
                'dbname': ''
            })
        }

    def patch(self):
        pass
