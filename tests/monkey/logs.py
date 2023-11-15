class Logs:
    def __init__(self, handler=None):
        self.logs = []
        if handler is not None:
            handler.log = self.log

    def log(self, *args, **kwargs):
        self.logs.append((args, kwargs))

    def find_logs_with_args(self, args):
        return [entry[0][0] for entry in self.logs if entry[0][0] == args]
