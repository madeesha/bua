import json
import uuid
from typing import Union, Dict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta

from bua.facade.ssm import SSM
from bua.handler import LambdaHandler


class BUANotifyHandler(LambdaHandler):

    def __init__(self, *, config, sqs_client, ddb_bua_table, failure_queue, sfn_client, ssm_client, debug=False):
        LambdaHandler.__init__(
            self, sqs_client=sqs_client, ddb_table=ddb_bua_table, debug=debug, failure_queue=failure_queue
        )
        self.config = config
        self.sfn_client = sfn_client
        self.ssm = SSM(ssm_client=ssm_client)
        self._default_handler = self._handle_event

        prefix = self.config['prefix']

        self.update_id_name = f"/{prefix}/bua/update_id"
        self.snapshot_arn_name = f"/{prefix}/bua/snapshot_arn"
        self.source_account_id_name = f"/{prefix}/bua/source_account_id"
        self.notify_steps_name = f"/{prefix}/bua/notify_steps"
        self.run_date_name = f"/{prefix}/bua/run_date"
        self.today_name = f"/{prefix}/bua/today"
        self.start_inclusive_name = f"/{prefix}/bua/start_inclusive"
        self.end_inclusive_name = f"/{prefix}/bua/end_inclusive"
        self.end_exclusive_name = f"/{prefix}/bua/end_exclusive"
        self.source_date_name = f"/{prefix}/bua/source_date"

    def _handle_event(self, body: Union[Dict, str]):

        if not isinstance(body, str):
            self.log(f'Expected a string arn')
            self.send_failure(str(body), f'Expected a string arn')
            return

        parameters = self._get_parameters()

        snapshot_arn = self._set_snapshot_arn(body, parameters)
        if snapshot_arn is None:
            self.log(f'Not an acceptable snapshot arn to use')
            self.send_failure(body, f'Not an acceptable snapshot arn to use')
            return

        self.log(f'Using snapshot_arn {snapshot_arn}')

        now = datetime.now(ZoneInfo('Australia/Sydney'))
        run_date = now.strftime('%Y-%m-%d')
        if body == 'reuse':
            self._reuse_date_parameters(parameters)
        else:
            self._set_new_date_parameters(run_date)

        update_id = self._increment_update_id(parameters)
        self.log(f'Using update_id {update_id}')

        if body == 'reuse':
            pipeline_steps = ''
        else:
            pipeline_steps = self._get_pipeline_steps(parameters)
        self.log(f'Using steps {pipeline_steps}')

        event = {
            'steps': pipeline_steps
        }

        state_machine_arn = self.config['state_machine_arn']
        self.log(f'Executing state machine {state_machine_arn}')

        unique_id = uuid.uuid4().hex
        id_part_1 = unique_id[0:8]
        id_part_2 = unique_id[8:12]
        id_part_3 = unique_id[12:16]
        id_part_4 = unique_id[16:20]
        id_part_5 = unique_id[20:]
        step_execution_name = f"{run_date}-Notify-{id_part_1}-{id_part_2}-{id_part_3}-{id_part_4}-{id_part_5}"
        self.log(f'Starting execution {step_execution_name}')

        self.sfn_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=step_execution_name,
            input=json.dumps(event)
        )

    def _set_new_date_parameters(self, run_date):
        self.ssm.put_parameter(self.run_date_name, run_date)
        self.log(f'Using run_date {run_date}')
        source_date = run_date
        self.ssm.put_parameter(self.source_date_name, source_date)
        self.log(f'Using source_date {source_date}')
        run_datetime = datetime.strptime(run_date, '%Y-%m-%d')
        today_datetime = run_datetime - timedelta(days=run_datetime.day - 1)
        today = today_datetime.strftime('%Y-%m-%d')
        self.ssm.put_parameter(self.today_name, today)
        self.log(f'Using today {today}')
        month_start_datetime = today_datetime - timedelta(days=today_datetime.day - 1)
        year_ago_datetime = month_start_datetime - relativedelta(years=1)
        end_inclusive_datetime = month_start_datetime - timedelta(days=1)
        start_inclusive = year_ago_datetime.strftime('%Y-%m-%d')
        self.ssm.put_parameter(self.start_inclusive_name, start_inclusive)
        self.log(f'Using start_inclusive {start_inclusive}')
        end_inclusive = end_inclusive_datetime.strftime('%Y-%m-%d')
        self.ssm.put_parameter(self.end_inclusive_name, end_inclusive)
        self.log(f'Using end_inclusive {end_inclusive}')
        end_exclusive = month_start_datetime.strftime('%Y-%m-%d')
        self.ssm.put_parameter(self.end_exclusive_name, end_exclusive)
        self.log(f'Using end_exclusive {end_exclusive}')

    def _reuse_date_parameters(self, parameters):
        last_run_date = parameters.get(self.run_date_name)
        self.log(f'Using run_date {last_run_date}')
        last_source_date = parameters.get(self.source_date_name)
        self.log(f'Using source_date {last_source_date}')
        last_today = parameters.get(self.today_name)
        self.log(f'Using today {last_today}')
        last_start_inclusive = parameters.get(self.start_inclusive_name)
        self.log(f'Using start_inclusive {last_start_inclusive}')
        last_end_inclusive = parameters.get(self.end_inclusive_name)
        self.log(f'Using end_inclusive {last_end_inclusive}')
        last_end_exclusive = parameters.get(self.end_exclusive_name)
        self.log(f'Using end_exclusive {last_end_exclusive}')

    def _get_parameters(self) -> Dict[str, str]:
        names = [
            self.update_id_name, self.snapshot_arn_name, self.source_account_id_name,
            self.notify_steps_name, self.run_date_name, self.today_name,
            self.start_inclusive_name, self.end_inclusive_name, self.end_exclusive_name,
            self.source_date_name
        ]
        return self.ssm.get_parameters(names)

    def _increment_update_id(self, parameters: Dict[str, str]):
        update_id = int(parameters[self.update_id_name])
        update_id += 1
        self.ssm.put_parameter(self.update_id_name, str(update_id))
        return update_id

    def _set_snapshot_arn(self, new_snapshot_arn: str, parameters: Dict[str, str]):
        new_snapshot_arn = new_snapshot_arn.strip()
        old_snapshot_arn = parameters[self.snapshot_arn_name]
        source_account_id = parameters[self.source_account_id_name]
        if len(old_snapshot_arn) > 0:
            if old_snapshot_arn == new_snapshot_arn:
                return old_snapshot_arn
            if new_snapshot_arn == 'reuse':
                return old_snapshot_arn
        if self._valid_arn(new_snapshot_arn, source_account_id):
            self.ssm.put_parameter(self.snapshot_arn_name, new_snapshot_arn)
            return new_snapshot_arn
        return None

    def _get_pipeline_steps(self, parameters: Dict[str, str]):
        notify_steps = parameters[self.notify_steps_name]
        if notify_steps == 'not-set':
            return ""
        return notify_steps

    def _valid_arn(self, snapshot_arn: str, source_account_id: str):
        aws_account_id = self.config['aws_account_id']
        aws_region = self.config['aws_region']
        arn = snapshot_arn.split(':')
        if len(arn) < 7:
            return False
        if arn[3] != aws_region:
            return False
        if arn[4] == source_account_id:
            return True
        if arn[4] == aws_account_id:
            return True
        return False
