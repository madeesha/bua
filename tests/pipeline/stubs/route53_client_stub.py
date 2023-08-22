class Route53ClientStub:

    def __init__(self, hosted_zone_id, start_record_name, start_record_type, resource_records):
        self.hosted_zone_id = hosted_zone_id
        self.start_record_name = start_record_name
        self.start_record_type = start_record_type
        self.resource_records = resource_records
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
                            "ResourceRecords": self.resource_records
                        }
                    }
                ]
            }
        }

    def list_resource_record_sets(self, **kwargs):
        for k, v in kwargs.items():
            assert self.expected_list_resource_record_sets[k] == v, f'Unexpected value {v} for key {k}'
        return {
            'ResourceRecordSets': [

            ]
        }

    def change_resource_record_sets(self, **kwargs):
        for k, v in kwargs.items():
            assert self.expected_change_resource_record_sets[k] == v, f'Unexpected value {v} for key {k}'
