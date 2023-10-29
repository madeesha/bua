from typing import List, Dict


class SSM:
    def __init__(self, ssm_client):
        self.ssm_client = ssm_client

    def get_parameters(self, names: List[str]) -> Dict[str, str]:
        response = self.ssm_client.get_parameters(Names=names, WithDecryption=True)
        if 'InvalidParameters' in response:
            invalid_parameters = response['InvalidParameters']
            if len(invalid_parameters) > 0:
                raise ValueError(f'Invalid parameters {invalid_parameters}')
        if 'Parameters' in response:
            return {param['Name']: param['Value'] for param in response['Parameters']}
        return dict()

    def put_parameter(self, name: str, value: str):
        self.ssm_client.put_parameter(Name=name, Value=value, Type='String', Overwrite=True, DataType='text')
