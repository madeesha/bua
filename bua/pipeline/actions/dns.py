from bua.pipeline.facade.route53 import Route53


class DNS:
    def __init__(self, config, route53: Route53):
        self.prefix = config['prefix']
        self.route53 = route53

    def set_rds_dns_entry(self, step, data):
        hosted_zone_id = data['hosted_zone_id']
        route53_records = data['route53_records']
        domain = data['domain']
        suffix = data['suffix']
        update_id = data['update_id']
        values = [f'{self.prefix}-{update_id}-{suffix}.{domain}']
        for record in route53_records:
            record_name = record['name']
            record_type = record['type']
            self.route53.set_dns_entry(hosted_zone_id, record_name, record_type, values)
        msg = f'RDS DNS set to {",".join(values)}'
        return "COMPLETE", msg
