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
                if state['Type'] == 'Choice':
                    assert state['Default'] in fsa['States']
                    for choice in state['Choices']:
                        assert choice['Next'] in fsa['States']

    def test_machines(self):
        entries = os.listdir(os.path.join(os.path.dirname(__file__), '..', '..', 'states'))
        for entry in entries:
            if entry.endswith('.json'):
                self._test_machine(entry)
        assert len(entries) > 0
