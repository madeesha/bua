from bua.facade.ssm import SSM
from bua.pipeline.handler.request import HandlerRequest


class ParameterActions:
    def __init__(self, ssm: SSM):
        self.ssm = ssm

    def get_parameters(self, request: HandlerRequest):
        data = request.data
        names = data['names']
        try:
            params = self.ssm.get_parameters(names=names)
            for key, value in params.items():
                data[key] = value
            return "COMPLETE", f"Retrieved {len(params)} parameters"
        except ValueError as ex:
            return "FAILED", f"Failed to retrieve parameters: {ex}"
