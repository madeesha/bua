import json
import os


class TestCase:

    @staticmethod
    def _test_machine(machine_name):
        with open(os.path.join(os.path.dirname(__file__), '..', '..', 'states', machine_name), 'r') as fp:
            fsa = json.load(fp)
            TestCase._validate_startat_exists(fsa)
            for name, state in fsa['States'].items():
                TestCase._validate_next_is_valid(fsa, state)
                TestCase._validate_resultselector(state, machine_name, name)
                TestCase._validate_choices(fsa, state)
                TestCase._validate_parameters(machine_name, name, state)
            TestCase._validate_states_usage(fsa)

    @staticmethod
    def _validate_states_usage(fsa):
        unused_states = set(fsa['States'].keys())
        TestCase._detect_used_states(unused_states, fsa['StartAt'], fsa['States'])
        assert len(unused_states) == 0, f'{unused_states} were not used'

    @staticmethod
    def _detect_used_states(unused_states: set, state_name: str, states: dict):
        unused_states.remove(state_name)
        current_state = states[state_name]
        if 'Next' in current_state:
            if current_state['Next'] in unused_states:
                TestCase._detect_used_states(unused_states, current_state['Next'], states)
        if current_state['Type'] == 'Choice':
            for choice in current_state['Choices']:
                if choice['Next'] in unused_states:
                    TestCase._detect_used_states(unused_states, choice['Next'], states)
            if 'Default' in current_state:
                if current_state['Default'] in unused_states:
                    TestCase._detect_used_states(unused_states, current_state['Default'], states)

    @staticmethod
    def _validate_parameters(machine_name, name, state):
        if 'Parameters' in state:
            key_name = f'{machine_name}: {name}/Parameters'
            TestCase._validate_jsonpath(key_name, state['Parameters'])

    @staticmethod
    def _validate_choices(fsa, state):
        if state['Type'] == 'Choice':
            assert state['Default'] in fsa['States']
            for choice in state['Choices']:
                assert choice['Next'] in fsa['States']

    @staticmethod
    def _validate_resultselector(state, machine_name, name):
        assert 'ResultSelector' not in state or state['Type'] in ('Task', 'Map', 'Parallel')
        if 'ResultSelector' in state:
            key_name = f'{machine_name}: {name}/ResultSelector'
            TestCase._validate_jsonpath(key_name, state['ResultSelector'])

    @staticmethod
    def _validate_next_is_valid(fsa, state):
        assert 'Next' not in state or state['Next'] in fsa['States']

    @staticmethod
    def _validate_startat_exists(fsa):
        assert fsa['StartAt'] in fsa['States']

    @staticmethod
    def _validate_jsonpath(name, parameters):
        for key, value in parameters.items():
            if isinstance(value, dict):
                TestCase._validate_jsonpath(f'{name}/{key}', value)
            if isinstance(value, str):
                if value.startswith('$.'):
                    assert key.endswith('.$'), f'{name}/{key} missing .$ on the key'
                if key.endswith('.$'):
                    _valid_json_path = value.startswith('$.') \
                                       or value == '$' \
                                       or value.startswith('States.') \
                                       or value.startswith('$$.')
                    assert _valid_json_path, f'{name}/{key} missing $. on the value'

    def test_machines(self):
        entries = os.listdir(os.path.join(os.path.dirname(__file__), '..', '..', 'states'))
        for entry in entries:
            if entry.endswith('.json'):
                self._test_machine(entry)
        assert len(entries) > 0
