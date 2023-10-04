from bua.facade.s3 import S3
from bua.pipeline.handler.request import HandlerRequest


class S3Actions:

    def __init__(self, *, s3: S3):
        self.s3 = s3

    def copy_s3_objects(self, request: HandlerRequest):
        data = request.data
        run_date = data['run_date']
        source_bucket = data['source_bucket']
        source_prefix = data['source_prefix'].replace('{run_date}', run_date[0:10]).replace('-', '')
        target_bucket = data['target_bucket']
        target_prefix = data['target_prefix'].replace('{run_date}', run_date[0:10]).replace('-', '')
        count = 0
        keys = self.s3.list_objects(
            source_bucket=source_bucket, source_prefix=source_prefix
        )
        copy_args = {key: data[key] for key in self.s3.copy_optionals.keys() if key in data}
        for source_key in keys:
            key_suffix = source_key[len(source_prefix):]
            target_key = f'{target_prefix}{key_suffix}'
            print(f'Copy {source_bucket}/{source_key} to {target_bucket}/{target_key}')
            self.s3.copy_object(
                source_bucket=source_bucket, source_key=source_key,
                target_bucket=target_bucket, target_key=target_key,
                **copy_args
            )
            count += 1
        status = "COMPLETE"
        reason = f'Copied {count} objects from {source_bucket}/{source_prefix} to {target_bucket}/{target_prefix}'
        return status, reason
