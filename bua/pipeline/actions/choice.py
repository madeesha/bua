class Choice:

    def choice(self, step, data):
        choices = data['choices']
        for choice in choices:
            if 'variable' in choice:
                variable = choice['variable']
                value = data[variable]
                if 'string_equals' in choice:
                    if value == choice['string_equals']:
                        msg = f'{variable} was {value}'
                        return choice['result'], msg
        return 'FAILED', 'No choice was matched'
