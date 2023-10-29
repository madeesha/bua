class MonkeyPatchStepFunctionsClient:
    def __init__(self):
        self._start_execution = []

    def patch(self):
        self._start_execution = []

    def start_execution(self, *args, **kwargs):
        self._start_execution.append((args, kwargs))
        assert 'stateMachineArn' in kwargs
        assert 'name' in kwargs
        assert 'input' in kwargs

    def assert_n_start_executions(self, n=0):
        assert len(self._start_execution) == n, self._start_execution

    def start_executions_startswith(self, n: int, key: str, value: str):
        assert len(self._start_execution) > n
        assert key in self._start_execution[n][1]
        actual = self._start_execution[n][1][key]
        assert actual.startswith(value), actual
