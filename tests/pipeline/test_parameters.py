from bua.facade.ssm import SSM
from bua.pipeline.actions.parameters import ParameterActions
from bua.pipeline.handler.request import HandlerRequest
from tests.pipeline.stubs.ssm_client_stub import SSMClientStub


class TestCase:

    def test_parameters(self):
        config = {
            'prefix': 'tst-buggy'
        }
        parameters = {
            '/tst-buggy/bua/abc': 'def'
        }
        ssm_client = SSMClientStub(parameters=parameters)
        ssm = SSM(ssm_client=ssm_client)
        uut = ParameterActions(config=config, ssm=ssm)
        event = {}
        step = {}
        data = {
            'names': [
                'abc'
            ]
        }
        request = HandlerRequest(event=event, step=step, data=data)
        status, reason = uut.get_parameters(request=request)
        assert status == 'COMPLETE'
        assert reason == 'Retrieved 1 parameters'
        assert data['abc'] == 'def'

    def test_missing_parameters(self):
        config = {
            'prefix': 'tst-buggy'
        }
        parameters = {
            '/tst-buggy/bua/abc': 'def'
        }
        ssm_client = SSMClientStub(parameters=parameters)
        ssm = SSM(ssm_client=ssm_client)
        uut = ParameterActions(config=config, ssm=ssm)
        event = {}
        step = {}
        data = {
            'names': [
                'def'
            ]
        }
        request = HandlerRequest(event=event, step=step, data=data)
        status, reason = uut.get_parameters(request=request)
        assert status == 'FAILED'
        assert reason == "Failed to retrieve parameters: Invalid parameters ['/tst-buggy/bua/def']"
