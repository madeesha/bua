class STSClientServiceIdStub:
    def hyphenize(self):
        pass


class STSClientServiceModelStub:
    def __init__(self):
        self.service_id = STSClientServiceIdStub()


class STSClientMetaStub:
    def __init__(self):
        self.service_model = STSClientServiceModelStub()


class STSClientStub:
    def __init__(self):
        self.meta = STSClientMetaStub()
