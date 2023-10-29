class MonkeyPatchSSMClient:

    def patch(self):
        self.get_parameters_requests = []
        self.put_parameter_requests = []
        self.parameters = dict()

    def put_parameter(self, *args, **kwargs):
        self.put_parameter_requests.append((args, kwargs))
        for key in kwargs.keys():
            assert key in {'Name', 'Value', 'Type', 'Overwrite', 'DataType'}
        self.parameters[kwargs['Name']] = kwargs['Value']
        return {}

    def get_parameters(self, *args, **kwargs):
        self.get_parameters_requests.append((args, kwargs))
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
