class Route53ClientStub:

    def __init__(self):
        self.hosted_zone_id = 'ZONEID'
        self.start_record_name = 'record1.here'
        self.start_record_type = 'CNAME'
        self.max_items = '1'

        self.expected_list_resource_record_sets = {
            'HostedZoneId': self.hosted_zone_id,
            'StartRecordName': self.start_record_name,
            'StartRecordType': self.start_record_type,
            'MaxItems': self.max_items,
        }

        self.expected_change_resource_record_sets = {
            'HostedZoneId': self.hosted_zone_id,
            'ChangeBatch': {
                "Changes": [
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": self.start_record_name,
                            "Type": self.start_record_type,
                            "TTL": 60,
                            "ResourceRecords": [
                                {
                                    "Value": 'abc.def'
                                }
                            ]
                        }
                    }
                ]
            }
        }

    def list_resource_record_sets(self, **kwargs):
        for k, v in kwargs.items():
            assert self.expected_list_resource_record_sets[k] == v
        return {
            'ResourceRecordSets': [

            ]
        }

    def change_resource_record_sets(self, **kwargs):
        for k, v in kwargs.items():
            assert self.expected_change_resource_record_sets[k] == v
