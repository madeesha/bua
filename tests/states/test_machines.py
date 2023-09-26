import json
import os


class TestCase:

    @staticmethod
    def _test_machine(machine_name):
        with open(os.path.join(os.path.dirname(__file__), '..', '..', 'states', machine_name), 'r') as fp:
            fsa = json.load(fp)
            assert fsa['StartAt'] in fsa['States']
            for name, state in fsa['States'].items():
                assert 'Next' not in state or state['Next'] in fsa['States']
                assert 'ResultSelector' not in state or state['Type'] in ('Task', 'Map', 'Parallel')
                if state['Type'] == 'Choice':
                    assert state['Default'] in fsa['States']
                    for choice in state['Choices']:
                        assert choice['Next'] in fsa['States']
                if 'Parameters' in state:
                    TestCase._validate_parameters(f'{name}/Parameters', state['Parameters'])

    @staticmethod
    def _validate_parameters(name, parameters):
        for key, value in parameters.items():
            if isinstance(value, dict):
                TestCase._validate_parameters(f'{name}/{key}', value)
            if isinstance(value, str):
                assert not value.startswith('$.') or key.endswith('.$'), f'{name}/{key} missing .$ on the key'

    def test_machines(self):
        entries = os.listdir(os.path.join(os.path.dirname(__file__), '..', '..', 'states'))
        for entry in entries:
            if entry.endswith('.json'):
                self._test_machine(entry)
        assert len(entries) > 0
