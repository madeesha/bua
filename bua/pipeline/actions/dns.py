from bua.facade.route53 import Route53
from bua.pipeline.handler.request import HandlerRequest


class DNS:
    def __init__(self, config, route53: Route53):
        self.prefix = config['prefix']
        self.route53 = route53

    def set_rds_dns_entry(self, request: HandlerRequest):
        data = request.data
        hosted_zone_id = data['hosted_zone_id']
        route53_records = data['route53_records']
        domain = data['domain']
        suffix = data['suffix']
        update_id = data['update_id']
        
        # Determine if this is Aurora or RDS based on snapshot ARN or explicit flag
        snapshot_arn = data.get('snapshot_arn', '')
        is_aurora = ':cluster-snapshot:' in snapshot_arn or data.get('database_type') == 'aurora'
        
        if is_aurora:
            # Aurora cluster endpoint format: <cluster-id>.cluster-<region>.rds.amazonaws.com
            # But we use the custom domain structure
            cluster_identifier = f'{self.prefix}-{update_id}-{suffix}'
            # Aurora clusters have a .cluster-<region>.rds.amazonaws.com endpoint
            # But if domain already contains the full RDS domain, use it directly
            if '.rds.amazonaws.com' in domain:
                # Extract region from domain if present
                # domain format: cmil2mssslzz.ap-southeast-2.rds.amazonaws.com
                values = [f'{cluster_identifier}.cluster-{domain}']
            else:
                # Custom domain - append cluster endpoint
                values = [f'{cluster_identifier}.{domain}']
        else:
            # RDS instance
            values = [f'{self.prefix}-{update_id}-{suffix}.{domain}']
        
        for record in route53_records:
            record_name = record['name']
            record_type = record['type']
            self.route53.set_dns_entry(hosted_zone_id, record_name, record_type, values)
        msg = f'DNS set to {",".join(values)}'
        return "COMPLETE", msg