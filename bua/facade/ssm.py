from typing import List, Dict


class SSM:
    def __init__(self, ssm_client):
        self.ssm_client = ssm_client

    def get_parameters(self, names: List[str]) -> Dict[str, str]:
        param_values = dict()
        for segment in range(0, len(names), 10):
            call_names = names[segment: min(len(names), segment+10)]
            response = self.ssm_client.get_parameters(Names=call_names, WithDecryption=True)
            if 'InvalidParameters' in response:
                invalid_parameters = response['InvalidParameters']
                if len(invalid_parameters) > 0:
                    raise ValueError(f'Invalid parameters {invalid_parameters}')
            if 'Parameters' in response:
                for param in response['Parameters']:
                    param_values[param['Name']] = param['Value']
        return param_values

    def put_parameter(self, name: str, value: str):
        self.ssm_client.put_parameter(Name=name, Value=value, Type='String', Overwrite=True, DataType='text')
