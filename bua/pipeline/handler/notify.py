import json
import uuid
from typing import Union, Dict
from datetime import datetime
from zoneinfo import ZoneInfo

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

        run_date = datetime.now(ZoneInfo('Australia/Sydney')).strftime('%Y-%m-%d')
        if body == 'reuse':
            prefix = self.config['prefix']
            run_date_name = f"/{prefix}/bua/run_date"
            last_run_date = parameters.get(run_date_name)
            self.log(f'Using run_date {last_run_date}')
        else:
            self._update_run_date(run_date)
            self.log(f'Using run_date {run_date}')

        update_id = self._increment_update_id(parameters)
        self.log(f'Using update_id {update_id}')

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

    def _get_parameters(self) -> Dict[str, str]:
        prefix = self.config['prefix']
        update_id_name = f"/{prefix}/bua/update_id"
        snapshot_arn_name = f"/{prefix}/bua/snapshot_arn"
        source_account_id_name = f"/{prefix}/bua/source_account_id"
        notify_steps_name = f"/{prefix}/bua/notify_steps"
        run_date_name = f"/{prefix}/bua/run_date"
        names = [update_id_name, snapshot_arn_name, source_account_id_name, notify_steps_name, run_date_name]
        return self.ssm.get_parameters(names)

    def _update_run_date(self, run_date: str):
        prefix = self.config['prefix']
        run_date_name = f"/{prefix}/bua/run_date"
        self.ssm.put_parameter(run_date_name, run_date)

    def _increment_update_id(self, parameters: Dict[str, str]):
        prefix = self.config['prefix']
        update_id_name = f"/{prefix}/bua/update_id"
        update_id = int(parameters[update_id_name])
        update_id += 1
        self.ssm.put_parameter(update_id_name, str(update_id))
        return update_id

    def _set_snapshot_arn(self, new_snapshot_arn: str, parameters: Dict[str, str]):
        new_snapshot_arn = new_snapshot_arn.strip()
        prefix = self.config['prefix']
        snapshot_arn_name = f"/{prefix}/bua/snapshot_arn"
        source_account_id_name = f"/{prefix}/bua/source_account_id"
        old_snapshot_arn = parameters[snapshot_arn_name]
        source_account_id = parameters[source_account_id_name]
        if len(old_snapshot_arn) > 0:
            if old_snapshot_arn == new_snapshot_arn:
                return old_snapshot_arn
            if new_snapshot_arn == 'reuse':
                return old_snapshot_arn
        if self._valid_arn(new_snapshot_arn, source_account_id):
            self.ssm.put_parameter(snapshot_arn_name, new_snapshot_arn)
            return new_snapshot_arn
        return None

    def _get_pipeline_steps(self, parameters: Dict[str, str]):
        prefix = self.config['prefix']
        notify_steps_name = f"/{prefix}/bua/notify_steps"
        notify_steps = parameters[notify_steps_name]
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
