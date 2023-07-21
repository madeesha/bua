from typing import List

import boto3


class Route53:
    def __init__(self, r53):
        self.r53 = r53

    def set_dns_entry(self, hosted_zone_id: str, record_name: str, record_type: str, values: List[str]):
        print(f'Set {record_name} {record_type} to {values}')
        response = self.r53.list_resource_record_sets(
            HostedZoneId=hosted_zone_id,
            StartRecordName=record_name,
            StartRecordType=record_type,
            MaxItems='1'
        )
        if 'ResourceRecordSets' in response:
            for record_set in response['ResourceRecordSets']:
                records = [record['Value'] for record in record_set['ResourceRecords']]
                if set(values) == set(records):
                    print("Already", records)
                    return
        self.r53.change_resource_record_sets(
            HostedZoneId=hosted_zone_id,
            ChangeBatch={
                "Changes": [
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": record_name,
                            "Type": "CNAME",
                            "TTL": 60,
                            "ResourceRecords": [
                                {
                                    "Value": value
                                }
                                for value in values
                            ]
                        }
                    }
                ]
            }
        )


if __name__ == '__main__':
    session = boto3.Session(region_name='ap-southeast-2', profile_name='anstead')
    client = session.client('route53')
    route53 = Route53(client)
    route53.set_dns_entry('Z06477101FOH3N8B2WK6N', 'tst-sql1.anstead.encore.sh', 'CNAME', ['tst-anstead-12-bua-sql.cmil2mssslzz.ap-southeast-2.rds.amazonaws.com'])
