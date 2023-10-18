from typing import Dict


class SSMClientStub:

    def __init__(self, parameters: Dict):
        self.requests = []
        self.parameters = parameters

    def get_parameters(self, *args, **kwargs):
        self.requests.append((args, kwargs))
        for key in kwargs.keys():
            assert key in {'Names', 'WithDecryption'}
        parameters = []
        invalid_parameters = []
        for name in kwargs['Names']:
            if name in self.parameters:
                parameters.append({
                    'Name': name, 'Value': self.parameters[name]
                })
            else:
                invalid_parameters.append(name)
        response = {
            'Parameters': parameters,
            'InvalidParameters': invalid_parameters
        }
        return response
