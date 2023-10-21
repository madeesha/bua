from bua.pipeline.utils.substitutions import Substitutions


class TestCase:

    def test_config_prefix_substitution(self):
        config = {
            'prefix': 'b'
        }
        data = {
            'key': 'a{{prefix}}c'
        }
        Substitutions(config, data).substitute_values(data)
        assert data['key'] == 'abc'

    def test_source_run_date_substitution(self):
        config = {
            'prefix': 'b'
        }
        data = {
            'run_date': '2023-10-10 10:11:12',
            'key': 'X-{{run_date}}-X',
        }
        Substitutions(config, data).substitute_values(data)
        assert data['key'] == 'X-2023-10-10 10:11:12-X'

    def test_source_filter_short_substitution(self):
        config = {
            'prefix': 'b'
        }
        data = {
            'run_date': '2023-10-10 10:11:12',
            'key': 'X-{{run_date|short}}-X',
        }
        Substitutions(config, data).substitute_values(data)
        assert data['key'] == 'X-20231010-X'

    def test_source_filter_lower_substitution(self):
        config = {
            'prefix': 'B'
        }
        data = {
            'key': 'a{{prefix|lower}}c',
        }
        Substitutions(config, data).substitute_values(data)
        assert data['key'] == 'abc'

    def test_source_filter_upper_substitution(self):
        config = {
            'prefix': 'b'
        }
        data = {
            'key': 'A{{prefix|upper}}C',
        }
        Substitutions(config, data).substitute_values(data)
        assert data['key'] == 'ABC'

    def test_source_sub_dictionary_substitution(self):
        config = {
            'prefix': 'b'
        }
        data = {
            'key': {
                'subkey': 'a{{prefix}}c'
            }
        }
        Substitutions(config, data).substitute_values(data)
        assert data['key']['subkey'] == 'abc'

    def test_source_dictionary_in_list_substitution(self):
        config = {
            'prefix': 'b'
        }
        data = {
            'keys': [
                {
                    'subkey': 'a{{prefix}}c'
                }
            ]
        }
        Substitutions(config, data).substitute_values(data)
        assert data['keys'][0]['subkey'] == 'abc'
