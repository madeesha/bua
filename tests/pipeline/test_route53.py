from bua.facade.route53 import Route53
from tests.pipeline.stubs.route53_client_stub import Route53ClientStub
from pytest import fixture


class TestCase:

    @fixture(autouse=True)
    def hosted_zone_id(self):
        return 'ZONE123'

    @fixture(autouse=True)
    def dns_record_name(self):
        return 'mysql.anstead.encore,'

    @fixture(autouse=True)
    def rds_host_name(self):
        return 'my.rds.host.name'

    @fixture(autouse=True)
    def r53_client(self, hosted_zone_id, dns_record_name, rds_host_name):
        return Route53ClientStub(
            hosted_zone_id=hosted_zone_id,
            start_record_name=dns_record_name,
            start_record_type='CNAME',
            resource_records=[
                {
                    'Value': rds_host_name
                }
            ]
        )

    def test_set_dns_entry(self, r53_client, hosted_zone_id, dns_record_name, rds_host_name):
        route53 = Route53(r53_client=r53_client)
        route53.set_dns_entry(
            hosted_zone_id=hosted_zone_id,
            record_name=dns_record_name,
            record_type='CNAME',
            values=[rds_host_name]
        )
