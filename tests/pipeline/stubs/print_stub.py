class PrintStub:

    def __init__(self):
        self.lines = []

    def print(self, *args, **kwargs):
        self.lines.append(args)
        print(*args, **kwargs)

    def assert_has_line(self, *args):
        for line in self.lines:
            if line == args:
                return
        assert False, f'{args} not printed: {self.lines}'
