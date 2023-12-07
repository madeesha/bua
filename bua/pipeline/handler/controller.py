from zoneinfo import ZoneInfo

import kubernetes
import pymysql
import yaml
import traceback
from typing import Dict, Any
from datetime import datetime, timezone, timedelta

from dateutil.relativedelta import relativedelta

from bua.facade.s3 import S3
from bua.facade.ssm import SSM
from bua.pipeline.actions.choice import Choice
from bua.pipeline.actions.dns import DNS
from bua.pipeline.actions.initiator import Initiator
from bua.pipeline.actions.kube import KubeCtl
from bua.pipeline.actions.parameters import ParameterActions
from bua.pipeline.actions.profiles import Profiles
from bua.pipeline.actions.reset import Reset
from bua.pipeline.actions.restore import Restore
from bua.pipeline.actions.destroy import Destroy
from bua.pipeline.actions.s3actions import S3Actions
from bua.pipeline.actions.sql import SQL
from bua.pipeline.actions.changeset import ChangeSet
from bua.pipeline.actions.trigger import Trigger
from bua.facade.rds import RDS
from bua.facade.route53 import Route53
from bua.facade.sm import SecretManager
from bua.facade.sqs import SQS
from bua.pipeline.handler.request import HandlerRequest
from bua.pipeline.utils.substitutions import Substitutions


class BUAControllerHandler:

    def __init__(
            self, config, r53_client, sm_client, s3_client, ddb_bua_table, sqs_client, cf_client, rds_client,
            sts_client, eks_client, ssm_client, session, mysql=pymysql, kubes=kubernetes, print=print
    ):
        self.print = print
        self.config = config
        self.s3_client = s3_client
        self.ddb_table = ddb_bua_table
        self.sqs = SQS(sqs_client=sqs_client, ddb_table=ddb_bua_table)
        self.s3 = S3(s3_client=s3_client)
        self.ssm = SSM(ssm_client=ssm_client)
        secret_manager = SecretManager(sm_client=sm_client)
        sql_handler = SQL(config=config, s3_client=s3_client, secret_manager=secret_manager, mysql=mysql, print=print)
        rds_handler = RDS(rds_client=rds_client)
        reset_handler = Reset(config=config, rds=rds_handler, secret_manager=secret_manager)
        restore_handler = Restore(config=config, cf_client=cf_client, rds=rds_handler)
        destroy_handler = Destroy(config=config, cf_client=cf_client)
        change_set_handler = ChangeSet(config=config, cf_client=cf_client)
        kubectl_handler = KubeCtl(
            config=config, sts_client=sts_client, eks_client=eks_client, session=session, kubes=kubes
        )
        profiles_handler = Profiles(config=config, sqs=self.sqs, s3=self.s3)
        route53 = Route53(r53_client=r53_client)
        dns_handler = DNS(config=config, route53=route53)
        trigger_handler = Trigger(config=config, sqs=self.sqs)
        choice_handler = Choice()
        initiator_handler = Initiator(config=config, sqs=self.sqs)
        s3_actions = S3Actions(s3=self.s3)
        ssm_actions = ParameterActions(config=config, ssm=self.ssm)

        self.handlers: Dict[str, Any] = {
            'get_config': self.get_config,
            'get_stepfunction_arns': self.get_stepfunction_arns,
            'restore_database': restore_handler.restore_database,
            'check_restore_database': restore_handler.check_restore_database,
            'copy_snapshot': restore_handler.copy_snapshot,
            'create_snapshot': restore_handler.create_snapshot,
            'check_copy_snapshot': restore_handler.check_copy_snapshot,
            'destroy_database': destroy_handler.destroy_database,
            'check_destroy_database': destroy_handler.check_destroy_database,
            'reset_password': reset_handler.reset_password,
            'export_procedures': sql_handler.export_procedures,
            'import_procedures': sql_handler.import_procedures,
            'create_upgrade_version_change_set': change_set_handler.create_upgrade_version_change_set,
            'create_scale_change_set': change_set_handler.create_scale_change_set,
            'create_change_set': change_set_handler.create_change_set,
            'check_change_set_ready': change_set_handler.check_change_set_ready,
            'execute_change_set': change_set_handler.execute_change_set,
            'check_change_set_complete': change_set_handler.check_change_set_complete,
            'disable_workflow_schedules': sql_handler.disable_workflow_schedules,
            'disable_workflow_instances': sql_handler.disable_workflow_instances,
            'core_warm_database_statistics': sql_handler.core_warm_database_statistics,
            'core_warm_database_indexes': sql_handler.core_warm_database_indexes,
            'wait_for_workflows': sql_handler.wait_for_workflows,
            'check_bua_control': sql_handler.check_bua_control,
            'set_bua_account_id': sql_handler.set_bua_account_id,
            'resubmit_failed_workflows': sql_handler.resubmit_failed_workflows,
            'resubmit_timedout_workflows': sql_handler.resubmit_timedout_workflows,
            'wait_for_workflow_schedules': sql_handler.wait_for_workflow_schedules,
            'stats_sample_pages': sql_handler.stats_sample_pages,
            'get_max_workflow_instance': sql_handler.get_max_workflow_instance,
            'truncate_workflow_instance': sql_handler.truncate_workflow_instance,
            'scale_replicas': kubectl_handler.scale_replicas,
            'check_replicas': kubectl_handler.check_replicas,
            'scale_down': kubectl_handler.scale_down,
            'bua_initiate': initiator_handler.bua_initiate,
            'bua_resolve_variances': sql_handler.bua_resolve_variances,
            'wait_for_empty_site_queues': profiles_handler.wait_for_empty_site_queues,
            'empty_site_errors_queues': profiles_handler.empty_site_errors_queues,
            'record_site_errors_queues': profiles_handler.record_site_errors_queues,
            'dump_site_errors_queues_to_s3': profiles_handler.dump_site_errors_queues_to_s3,
            'execute_sql': sql_handler.execute_sql,
            'ili_manual_line_item_exceptions': sql_handler.ili_manual_line_item_exceptions,
            'clean_site_data': sql_handler.clean_site_data,
            'insert_event_log': sql_handler.insert_event_log,
            'set_rds_dns_entry': dns_handler.set_rds_dns_entry,
            'trigger_restore': trigger_handler.trigger_restore,
            'bua_create_invoice_scalar': sql_handler.bua_create_invoice_scalar,
            'bua_initiate_invoice_runs': sql_handler.bua_initiate_invoice_runs,
            'bua_prepare_billing_threshold': sql_handler.bua_prepare_billing_threshold,
            'choice': choice_handler.choice,
            'scale_nodegroup': kubectl_handler.scale_nodegroup,
            'wait_for_scale_nodegroup': kubectl_handler.wait_for_scale_nodegroup,
            'copy_s3_objects': s3_actions.copy_s3_objects,
            'remove_s3_objects': s3_actions.remove_s3_objects,
            'bua_create_macro_profile': sql_handler.bua_create_macro_profile,
            'get_parameters': ssm_actions.get_parameters,
        }

    def get_config(self, request: HandlerRequest):
        data = request.data
        data['config'] = dict(self.config)
        region = data["config"]["region"]
        account = data["config"]["aws_account"]
        prefix = data["config"]["prefix"]
        data['config']['states_prefix'] = f'arn:aws:states:{region}:{account}:stateMachine:{prefix}")'
        return "COMPLETE", f'Retrieved config values'

    def get_stepfunction_arns(self, request: HandlerRequest):
        event = request.event
        region = self.config['region']
        account = self.config['aws_account']
        prefix = self.config['prefix']
        stepfunctions = event.get('stepfunction', dict())
        for name in stepfunctions:
            event['stepfunction'][name] = f'arn:aws:states:{region}:{account}:stateMachine:{prefix}-{name}'
        return "COMPLETE", f'Calculated {len(stepfunctions)} stepfunction names'

    def handle_request(self, event: Dict):
        self.print('ProjectVersion', self.config['version'])
        if 'Records' in event:
            for record in event['Records']:
                self.print(record)
                if record['eventSource'] == 'aws:s3':
                    text = self._fetch_s3_object(record)
                    body = yaml.load(text, Loader=yaml.Loader)
                    self.handle_request(body)
                if record['eventSource'] == 'aws:sqs':
                    if self.sqs.deduplicate_request(record):
                        body = yaml.load(record['body'], Loader=yaml.Loader)
                        if 'Records' in body:
                            self.handle_request(body)
                        else:
                            body['type'] = body.get('type', 'sqs')
                            self._handle_event(body)
        else:
            event['type'] = event.get('type', 'sqs')
            self._handle_event(event)
            return event

    def _fetch_s3_object(self, record):
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        version = record['s3']['object']['versionId']
        response = self.s3_client.get_object(Bucket=bucket, Key=key, VersionId=version)
        text: str = response['Body'].read().decode('utf-8')
        return text

    def _handle_event(self, event: Dict):
        use_sqs = event['type'] == 'sqs'
        try:
            if 'action' in event:
                self._handle_action(event)
            else:
                self._handle_pipeline(event, use_sqs)
        except Exception as e:
            self._handle_step_failure(e, event, use_sqs)

    def _handle_action(self, event: Dict):
        name: str = event.get('name', event['action'].replace('_', ' ').title())
        this: str = event['action']
        self._handle_step(event, name, this, event)

    def _handle_pipeline(self, event: Dict, use_sqs: bool):
        if 'name' not in event:
            raise Exception('[name] tag is missing from the event')
        name = event['name']
        if 'this' not in event:
            raise Exception('[this] tag is missing from the event')
        this = event['this']
        if 'steps' in event:
            self._handle_event_steps(event, name, this)
        else:
            self._handle_step(event, name, this, event)
        if use_sqs:
            self._handle_next_via_sqs(event)

    def _handle_next_via_sqs(self, event: Dict):
        _next = event.get('next')
        if _next is not None and len(_next) > 0:
            event['this'] = _next
            del event['next']
            delay = event.get('delay', 0)
            if 'delay' in event:
                del event['delay']
            response = self.sqs.send_message(
                queue_url=self.config['next_queue_url'],
                body=yaml.dump(event),
                delay=delay
            )
            self.print(f'Sent message [{response["MessageId"]}] to {self.config["next_queue_url"]}')

    def _handle_event_steps(self, event: Dict, name: str, this: str):
        if this not in event['steps']:
            raise Exception(f'There is no step definition for [{this}] in the event')
        step = event['steps'][this]
        self._handle_step(event, name, this, step)

    def _handle_step(self, event: Dict, name: str, this: str, step: Dict):
        event['next'] = ''
        event['speed'] = 'fast'
        event['delay'] = 0

        data = self._get_data(event)
        self._calculate_run_dates(data)
        args = step.get('args')
        if args is not None:
            Substitutions(self.config, data).substitute_values(args)
            self._process_args(data, step)
        Substitutions(self.config, data).substitute_values(data)

        instance = self._determine_instance(data)
        log_item = self._log_processing_start(instance, name, this)
        status, reason = self._check_retries_exceeded(step)
        if status == 'OK':
            reason, status = self._perform_action(log_item, this, event, step, data)
        status, reason = self._determine_next_step(event, log_item, status, step, reason)
        self._record_result(event, log_item, reason, status, this)
        self.ddb_table.put_item(Item=log_item)
        if status == 'ABORT':
            raise Exception(reason)

    @staticmethod
    def _calculate_run_dates(data):
        run_date = datetime.now(ZoneInfo('Australia/Sydney'))
        data['current_date'] = run_date.strftime('%Y-%m-%d')
        data['current_time'] = run_date.strftime('%H:%M:%S')
        if 'run_date' not in data or data['run_date'] is None or len(data['run_date']) == 0:
            data['run_date'] = data['current_date']
        else:
            if len(data['run_date']) == 10:
                run_date = datetime.strptime(data['run_date'], '%Y-%m-%d')
            else:
                run_date = datetime.strptime(data['run_date'][0:19], '%Y-%m-%d %H:%M:%S')
        if 'today' not in data or data['today'] is None or len(data['today']) == 0:
            today = run_date - timedelta(days=run_date.day - 1)
            data['today'] = today.strftime('%Y-%m-%d')
            today = datetime.strptime(data['today'], '%Y-%m-%d')
        else:
            today = datetime.strptime(data['today'], '%Y-%m-%d')
        if 'source_date' not in data or data['source_date'] is None or len(data['source_date']) == 0:
            data['source_date'] = data['run_date']
        month_start = today - timedelta(days=today.day - 1)
        year_ago = month_start - relativedelta(years=1)
        end_inclusive = month_start - timedelta(days=1)
        if 'start_inclusive' not in data or data['start_inclusive'] is None or len(data['start_inclusive']) == 0:
            data['start_inclusive'] = year_ago.strftime('%Y-%m-%d')
        if 'end_exclusive' not in data or data['end_exclusive'] is None or len(data['end_exclusive']) == 0:
            data['end_exclusive'] = month_start.strftime('%Y-%m-%d')
        if 'end_inclusive' not in data or data['end_inclusive'] is None or len(data['end_inclusive']) == 0:
            data['end_inclusive'] = end_inclusive.strftime('%Y-%m-%d')

    def _handle_step_failure(self, e, event: Dict, use_sqs: bool):
        traceback.print_exception(e)
        if use_sqs:
            response = self.sqs.send_message(
                queue_url=self.config['failure_queue_url'],
                body=yaml.dump({
                    'event': event,
                    'cause': traceback.format_exception(e)
                })
            )
            self.print(f'Sent message [{response["MessageId"]}] to {self.config["failure_queue_url"]}')
        else:
            raise

    def _perform_action(self, log_item: Dict, this: str, event: Dict, step: Dict, data: Dict):
        if 'action' in step:

            action = step['action']
            log_item['ACTION'] = action
            self.print(f'Invoking [{action}] for [{this}]')

            try:
                if action not in self.handlers:
                    raise Exception(f'Cannot find handler for [{action}]')
                status, reason = self.handlers[action](HandlerRequest(event, step, data))
            except Exception as e:
                log_item['TIME2'] = self._local_time()
                log_item['STATUS'] = 'ABORT'
                log_item['REASON'] = str(e)
                self.ddb_table.put_item(Item=log_item)
                raise

        else:
            status, reason = 'COMPLETE', 'No action'
        return reason, status

    def _record_result(self, event: Dict, log_item: Dict, reason: str, status: str, this: str):
        log_item['TIME2'] = self._local_time()
        if status is not None:
            log_item['STATUS'] = status
        if reason is not None:
            log_item['REASON'] = reason
        event['result'] = {
            'step': this,
            'status': status,
            'reason': reason,
            'when': log_item['TIME2']
        }
        self.print(f'Result : [{status}] : {reason}')

    @staticmethod
    def _local_time():
        local_now = datetime.now(ZoneInfo('Australia/Sydney')).replace(microsecond=0)
        return local_now.strftime('%Y-%m-%d %H:%M:%S')

    def _log_processing_start(self, instance: str, name: str, this: str):
        time1 = self._local_time()
        result = self.ddb_table.get_item(
            Key={
                'PK': name,
                'SK': this
            },
            ConsistentRead=True
        )
        if 'Item' in result:
            if 'INSTANCE' in result['Item'] and result['Item']['INSTANCE'] == instance:
                if 'TIME1' in result['Item']:
                    time1 = result['Item']['TIME1']
        log_item = {
            'PK': name,
            'SK': this,
            'INSTANCE': instance,
            'TIME1': time1,
        }
        self.ddb_table.put_item(Item=log_item)
        return log_item

    @staticmethod
    def _determine_instance(data: Dict) -> str:
        if 'instance' not in data or data['instance'] is None or len(data['instance']) == 0:
            data['instance'] = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        instance = str(data['instance'])
        return instance

    @staticmethod
    def _process_args(data: Dict, step: Dict):
        if 'args' in step:
            for key, value in step['args'].items():
                data[key] = value

    @staticmethod
    def _get_data(event: Dict) -> Dict:
        if 'data' not in event:
            event['data'] = dict()
        data = event['data']
        return data

    def _determine_next_step(self, event: Dict, log_item: Dict, status: str, step: Dict, reason: str):
        if 'on' in step:
            if status in step['on']:
                if 'next' in step['on'][status]:
                    event['next'] = step['on'][status]['next']
                    event['delay'] = int(step['on'][status].get('delay', 0))
                    if event['next'] in event['steps']:
                        event['speed'] = event['steps'][event['next']].get('speed', 'fast')
                        self._log_next_step(event, log_item)
                    else:
                        status = 'TERMINATE'
                        reason = f"Misconfigured next step {event['next']}"
                        self._no_next_step(event, log_item)
                else:
                    self._no_next_step(event, log_item)
                return status, reason
        if status == 'RETRY':
            event['next'] = event['this']
            event['delay'] = int(step.get('retry_delay', event.get('retry_delay', 60)))
            self._log_next_step(event, log_item)
            return status, reason
        if status not in ('ABORT', 'TERMINATE'):
            log_item['WARNING'] = f'Unhandled state transition {status}'
        self._no_next_step(event, log_item)
        return status, reason

    @staticmethod
    def _check_retries_exceeded(step: Dict):
        status = 'OK'
        reason = None
        if 'retries' in step:
            retries = int(step['retries'])
            if retries >= 0:
                step['retries'] = retries - 1
            else:
                status = 'EXPIRED'
                reason = f'Retries exceeded'
        return status, reason

    @staticmethod
    def _log_next_step(event: Dict, log_item: Dict):
        if len(event['next']) > 0:
            log_item['NEXT'] = event['next']
        elif 'NEXT' in log_item:
            del log_item['NEXT']
        if event['delay'] > 0:
            log_item['DELAY'] = str(event['delay'])
        elif 'DELAY' in log_item:
            del log_item['DELAY']

    def _no_next_step(self, event: Dict, log_item: Dict):
        event['next'] = ''
        event['delay'] = 0
        self._log_next_step(event, log_item)
