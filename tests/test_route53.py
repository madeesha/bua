from bua.route53 import Route53
from tests.route53_client_stub import Route53ClientStub


class TestCase:

    def test_set_dns_entry(self):
        r53_client = Route53ClientStub()
        route53 = Route53(r53_client=r53_client)
        route53.set_dns_entry(
            hosted_zone_id='ZONEID', record_name='record1.here', record_type='CNAME', values=['abc.def']
        )
