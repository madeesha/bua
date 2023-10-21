from typing import Dict, Optional


class Substitutions:

    def __init__(self, config: Dict, source: Dict):
        self.config = config
        self.source = source

    def substitute_values(self, data: Dict):
        for key, value in data.items():
            if isinstance(value, str):
                data[key] = self.substitute_string_value(value)
            if isinstance(value, dict):
                self.substitute_values( value)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self.substitute_values(item)

    def substitute_string_value(self, value: str) -> str:
        sub_start = value.find('{{')
        while sub_start >= 0:
            sub_end = value.find('}}', sub_start+2)
            if sub_end < sub_start:
                break
            prefix = value[0:sub_start]
            match = value[sub_start+2:sub_end]
            suffix = value[sub_end+2:]
            filter_index = match.find('|')
            if filter_index > 0:
                filter = match[filter_index+1:]
                match = match[0:filter_index]
            else:
                filter = None
            if match in self.config:
                value = prefix + self.filter_value(filter, self.config[match]) + suffix
            elif match in self.source:
                value = prefix + self.filter_value(filter, self.source[match]) + suffix
            else:
                raise ValueError(f'[{match}] is not found in config or source')
        return value

    @staticmethod
    def filter_value(filter: Optional[str], value: str) -> str:
        if filter is None:
            return value
        if filter == 'short':
            return value[0:10].replace('-','')
        if filter == 'lower':
            return value.lower()
        if filter == 'upper':
            return value.upper()
        raise ValueError(f'Filter {filter} is not known')
