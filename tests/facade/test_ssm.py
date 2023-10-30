from bua.facade.ssm import SSM
import tests.monkey.patch as monkey_patch


class TestCase:

    def test_get_parameters_more_than_10(self):
        environ = {}
        monkey_patch.patch.patch(environ=environ)
        ssm_client = monkey_patch.patch.ssm()
        keys = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        ssm_client.parameters = {n: n for n in keys}
        ssm = SSM(ssm_client=ssm_client)
        params = ssm.get_parameters(names=[n for n in keys])
        assert params == {n: n for n in keys}
        assert len(ssm_client.get_parameters_requests) == 3
