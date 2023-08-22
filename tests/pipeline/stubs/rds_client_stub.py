class RDSClientStub:
    def modify_db_instance(self, *args, **kwargs):
        for key in kwargs.keys():
            assert key in {'DBInstanceIdentifier', 'MasterUserPassword'}
        return {}
