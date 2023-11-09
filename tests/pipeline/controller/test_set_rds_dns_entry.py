from tests.pipeline.controller.base import TestBase


class TestCase(TestBase):

    def test_set_rds_dns_entry(self, handler, sqs, hosted_zone_id, dns_record_name, rds_domain_name, suffix, update_id):
        body = {
            'name': 'Restore Database',
            'this': 'step1',
            'data': {
                'hosted_zone_id': hosted_zone_id,
                'update_id': update_id,
                'suffix': suffix,
                'domain': rds_domain_name,
            },
            'steps': {
                'step1': {
                    'action': 'set_rds_dns_entry',
                    'args': {
                        'route53_records': [
                            {
                                'name': dns_record_name,
                                'type': 'CNAME'
                            }
                        ]
                    }
                }
            }
        }
        handler.handle_request(body)
        sqs.assert_no_failures()
