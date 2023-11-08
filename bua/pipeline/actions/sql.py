import base64
from hashlib import md5
import re
from typing import Dict, List

import pymysql
import pymysql.cursors

from bua.facade.sm import SecretManager
from bua.pipeline.handler.request import HandlerRequest


class SQL:

    def __init__(self, config, s3_client, secret_manager: SecretManager, mysql=pymysql):
        self.config = config
        self.s3 = s3_client
        self.sm = secret_manager
        self.prefix = config['prefix']
        self.mysql = mysql

    def _connect(self, data):
        update_id = data['update_id']
        suffix = data['suffix']
        domain = data['domain']
        schema = data['schema']
        secret = self.sm.fetch_secret(data['rdssecret'])
        username = secret['username']
        password = secret['password']
        db_instance_identifier = f'{self.prefix}-{update_id}-{suffix}'
        host = f'{db_instance_identifier}.{domain}'
        con = self.mysql.connect(
            host=host, user=username, password=password, database=schema,
            cursorclass=pymysql.cursors.DictCursor, autocommit=False
        )
        with con.cursor() as cur:
            cur.execute("SET SESSION innodb_lock_wait_timeout = 60")
        return con

    def insert_event_log(self, request: HandlerRequest):
        data = request.data
        events: List[Dict] = data['events']
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    workflow_instance_id = self._set_max_workflow_instance_id(cur, data)
                    con.commit()
                    for event in events:
                        key_set = sorted(event.keys())
                        keys = ','.join(key_set)
                        placeholders = ','.join(['%s'] * len(key_set))
                        stmt = f'INSERT INTO EventLog ({keys}) VALUES ({placeholders})'
                        print(stmt)
                        values = [event[key] for key in key_set]
                        cur.execute(stmt, values)
                    con.commit()
            return "COMPLETE", f'Inserted event log record. Max WFI {workflow_instance_id}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def execute_sql(self, request: HandlerRequest):
        data = request.data
        args = request.step.get('args', {})
        sql = args.get('sql')
        if sql is None:
            sql_secret_name = args.get('sql_secret_name')
            if sql_secret_name is not None:
                secret_value = self.sm.fetch_secret(sql_secret_name)
                if 'sql' in secret_value:
                    sql = secret_value['sql']
        if sql is None:
            return "FAILED", f"No SQL statements to execute"
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    for stmt in sql:
                        cur.execute(stmt)
                    con.commit()
            return "COMPLETE", f'Execute {len(sql)} SQL statements'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def bua_create_macro_profile(self, request: HandlerRequest):
        data = request.data
        end_inclusive = data['end_inclusive']
        run_date = data['run_date']
        identifier_type = data['identifier_type']
        stream_types = data['stream_types']
        jurisdiction_name = data.get('jurisdiction_name', 'ALL')
        account_type = data.get('account_type', 'ALL')
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    stmt = "CALL bua_create_macro_profile(%s, %s, %s, %s, %s, %s, 0)"
                    for stream_type in stream_types:
                        in_run_date = '2099-01-01' if stream_type == 'GAS' else run_date
                        params = (
                            end_inclusive, in_run_date, identifier_type, jurisdiction_name,
                            account_type, stream_type
                        )
                        cur.execute(stmt, params)
                    con.commit()
            return "COMPLETE", f'Executed bua_create_macro_profile for stream type {stream_type} on {run_date}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def bua_prepare_billing_threshold(self, request: HandlerRequest):
        data = request.data
        end_inclusive = data['end_inclusive']
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    stmt = f"CALL bua_prepare_billing_threshold(%s, 0)"
                    params = (end_inclusive,)
                    cur.execute(stmt, params)
                    con.commit()
            return "COMPLETE", f'BUA prepare billing threshold, end date {end_inclusive}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def bua_initiate_invoice_runs(self, request: HandlerRequest):
        data = request.data
        run_date = data.get('run_date')
        aws_account = self.config['aws_account']
        schedule_gap = data.get('schedule_gap', 15)
        min_days = data.get('min_days', 1)
        num_batches = data.get('num_batches', 80)
        concurrent_batches = data.get('concurrent_batches', 4)
        num_groups = data.get('num_groups', 8)
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    workflow_instance_id = self._set_max_workflow_instance_id(cur, data)
                    con.commit()
                    cur.execute(
                        "UPDATE GlobalSetting SET value = %s WHERE name = %s",
                        (num_batches, "INVRUN_BATCHES")
                    )
                    con.commit()
                    cur.execute(
                        "UPDATE GlobalSetting SET value = %s WHERE name = %s",
                        (concurrent_batches, "INVRUN_CONCURRENT_BATCH")
                    )
                    con.commit()
                    cur.execute(
                        "CALL bua_initiate_invoice_runs(%s, %s, %s, %s)",
                        (run_date, schedule_gap, min_days, num_groups)
                    )
                    con.commit()
            return "COMPLETE", f'BUA initiate invoice runs, max wfi {workflow_instance_id}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def bua_create_invoice_scalar(self, request: HandlerRequest):
        data = request.data
        concurrency = int(data['concurrency'])
        start_inclusive = data.get('start_inclusive')
        end_exclusive = data.get('end_exclusive')
        today = data.get('today')
        run_date = data.get('run_date')
        identifier_type = data['identifier_type']
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    workflow_instance_id = self._set_max_workflow_instance_id(cur, data)
                    con.commit()
                    cur.execute(
                        "CALL bua_create_invoice_scalar_bootstrap(%s, %s, %s, %s, %s, %s)",
                        (concurrency, start_inclusive, end_exclusive, today, run_date, identifier_type)
                    )
                    con.commit()
            return "COMPLETE", f'BUA create invoice scalar, max wfi {workflow_instance_id}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def bua_initiate(self, request: HandlerRequest):
        data = request.data
        run_type = data['run_type']
        start_inclusive = data.get('start_inclusive')
        end_inclusive = data.get('end_inclusive')
        today = data['today']
        run_date = data['run_date']
        source_date = data.get('source_date')
        identifier_type = data.get('identifier_type')
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    cur.execute(
                        "CALL bua_initiate(%s, %s, %s, %s, %s, %s, %s)",
                        (run_type, start_inclusive, end_inclusive, today, run_date, source_date, identifier_type)
                    )
                    con.commit()
            return "COMPLETE", f'Initiated BUA {run_type} as at {today}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def bua_resolve_variances(self, request: HandlerRequest):
        data = request.data
        run_date = data['run_date']
        source_date = data.get('source_date')
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    cur.execute(
                        "CALL bua_resolve_variances(%s, %s, 0)",
                        (run_date, source_date)
                    )
                    con.commit()
            return "COMPLETE", f'Initiated BUA resolve variances for {run_date}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def truncate_workflow_instance(self, request: HandlerRequest):
        data = request.data
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                    cur.execute("TRUNCATE TABLE InvoiceEventLog")
                    cur.execute("TRUNCATE TABLE EventLog")
                    cur.execute("TRUNCATE TABLE WorkflowInstance")
                    cur.execute("SET FOREIGN_KEY_CHECKS = 1")
                    con.commit()
            return "COMPLETE", f'Truncated workflow instance data'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def get_max_workflow_instance(self, request: HandlerRequest):
        data = request.data
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    workflow_instance_id = self._set_max_workflow_instance_id(cur, data)
            return "COMPLETE", f'Max workflow instance {workflow_instance_id}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def _set_max_workflow_instance_id(self, cur, data):
        cur.execute("SELECT COALESCE(MAX(id),0) AS id FROM WorkflowInstance")
        workflow_instance_id = int(cur.fetchall()[0]['id'])
        data['workflow_instance_id'] = workflow_instance_id
        print(f'Max workflow_instance_id is {workflow_instance_id}')
        return workflow_instance_id

    def set_bua_account_id(self, request: HandlerRequest):
        data = request.data
        aws_account = self.config['aws_account']
        try:
            con = self._connect(data)
            with con:
                cur: pymysql.cursors.SSDictCursor = con.cursor()
                with cur:
                    stmt = """
                    UPDATE GlobalSetting 
                    SET value = %s, active = 1
                    WHERE name IN ('BUA_AWS_ACCOUNT_ID', 'THIS_AWS_ACCOUNT_ID')
                    """
                    params = (aws_account,)
                    cur.execute(stmt, params)
                    con.commit()
            return "COMPLETE", f'Set bua account id {aws_account}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def check_bua_control(self, request: HandlerRequest):
        data = request.data
        run_type = data['run_type']
        run_date = data['run_date']
        args = request.step.get('args', dict())
        acceptable_error_rate = int(args.get('acceptable_error_rate', 0))
        max_errors = int(args.get('max_errors', 0))
        try:
            con = self._connect(data)
            with con:
                cur: pymysql.cursors.SSDictCursor = con.cursor()
                with cur:
                    sql = "SELECT status, COUNT(*) AS total " \
                          "FROM BUAControl " \
                          "WHERE run_type = %s AND run_date = %s " \
                          "GROUP BY status "
                    params = (run_type, run_date)
                    cur.execute(sql, params)
                    results = {row['status']: row['total'] for row in cur.fetchall()}
                    if 'FAIL' in results:
                        total_errors = results.get('FAIL', 0)
                        total_done = results.get('DONE', 0)
                        total_instances = total_done + total_errors
                        acceptable_errors = max(total_instances * acceptable_error_rate / 100, max_errors)
                        if total_errors > acceptable_errors:
                            return "FAILED", f'{results["FAIL"]} {run_type} in FAIL status for {run_date}'
            return "COMPLETE", f'No {run_type} in FAIL status for {run_date}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def wait_for_workflows(self, request: HandlerRequest):
        data = request.data
        args = request.step.get('args', dict())
        workflow_names = args['workflow_names']
        workflow_instance_id = int(args.get('workflow_instance_id', 0))
        acceptable_error_rate = int(args.get('acceptable_error_rate', 0))
        max_errors = int(args.get('max_errors', 0))
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    for workflow_name in workflow_names:
                        cur.execute("SELECT id FROM Workflows WHERE name = %s", (workflow_name,))
                        workflow_id = int(cur.fetchall()[0]['id'])
                        cur.execute(
                            "SELECT status, COUNT(*) AS total "
                            "FROM WorkflowInstance "
                            "WHERE workflow_id = %s "
                            "AND id > %s "
                            "GROUP BY status",
                            (workflow_id, workflow_instance_id)
                        )
                        results = {row['status']: row['total'] for row in cur.fetchall()}
                        if 'NEW' in results:
                            return "RETRY", f'{results["NEW"]} {workflow_name} workflows in NEW status'
                        if 'READY' in results:
                            return "RETRY", f'{results["READY"]} {workflow_name} workflows in READY status'
                        if 'INPROG' in results:
                            return "RETRY", f'{results["INPROG"]} {workflow_name} workflows in INPROG status'
                        if 'ERROR' in results:
                            total_errors = results.get('ERROR', 0)
                            total_done = results.get('DONE', 0)
                            total_instances = total_done + total_errors
                            acceptable_errors = max(total_instances * acceptable_error_rate / 100, max_errors)
                            if total_errors > acceptable_errors:
                                return "FAILED", f'{results["ERROR"]} {workflow_name} workflows in ERROR status'
                        if 'HOLD' in results:
                            return "ONHOLD", f'{results["HOLD"]} {workflow_name} workflows in HOLD status'
            return "COMPLETE", f'No workflows in NEW/READY/INPROG remain'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def resubmit_failed_workflows(self, request: HandlerRequest):
        data = request.data
        workflow_names = data['workflow_names']
        workflow_instance_id = int(data.get('workflow_instance_id', 0))
        try:
            total = 0
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    for workflow_name in workflow_names:
                        cur.execute("SELECT id FROM Workflows WHERE name = %s", (workflow_name,))
                        workflow_id = int(cur.fetchall()[0]['id'])
                        cur.execute(
                            "UPDATE WorkflowInstance "
                            "SET status = 'NEW' "
                            "WHERE status = 'ERROR' "
                            "AND workflow_id = %s "
                            "AND id > %s ",
                            (workflow_id, workflow_instance_id)
                        )
                        row_count = con.affected_rows()
                        total += row_count
                        print(f'Resubmitted {row_count} {workflow_name} workflow instances')
            return "COMPLETE", f'Resubmitted {total} workflow instances'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def wait_for_workflow_schedules(self, request: HandlerRequest):
        data = request.data
        args = request.step.get('args', dict())
        workflow_names = args['workflow_names']
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    for workflow_name in workflow_names:
                        cur.execute(
                            "SELECT COUNT(*) AS total "
                            "FROM WorkflowSchedule "
                            "WHERE workflow_id = (SELECT id FROM Workflows WHERE name = %s) "
                            "AND enabled = 1 "
                            "AND next_run_date < DATE_ADD(NOW(), INTERVAL 1 DAY)",
                            (workflow_name,)
                        )
                        count = int(cur.fetchall()[0]['total'])
                        if count > 0:
                            return "RETRY", f'{count} {workflow_name} schedules still to execute'
            return "COMPLETE", f'No workflow schedules still to execute'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def core_warm_database_statistics(self, request: HandlerRequest):
        data = request.data
        concurrency = int(data['concurrency'])
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    workflow_instance_id = self._set_max_workflow_instance_id(cur, data)
                    con.commit()
                    cur.execute("CALL core_warm_database_statistics(%s, 1)", (concurrency, ))
                    con.commit()

            return "COMPLETE", f'CORE warm database statistics, max wfi {workflow_instance_id}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def core_warm_database_indexes(self, request: HandlerRequest):
        data = request.data
        concurrency = int(data['concurrency'])
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    workflow_instance_id = self._set_max_workflow_instance_id(cur, data)
                    con.commit()
                    cur.execute("CALL core_warm_database_indexes(%s, 1)", (concurrency, ))
                    con.commit()
            return "COMPLETE", f'CORE warm database indexes, max wfi {workflow_instance_id}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def disable_workflow_schedules(self, request: HandlerRequest):
        data = request.data
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    cur.execute("UPDATE WorkflowSchedule SET enabled = 2 WHERE enabled = 1")
                    con.commit()
            return "COMPLETE", 'Disabled workflow schedules'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def disable_workflow_instances(self, request: HandlerRequest):
        data = request.data
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    cur.execute("UPDATE WorkflowInstance SET status = 'HOLD' WHERE status IN ('NEW', 'READY', 'INPROG')")
                    con.commit()
            return "COMPLETE", 'Disabled workflow instances'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def stats_sample_pages(self, request: HandlerRequest):
        data = request.data
        tables = data['tables']
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    for table in tables:
                        name = table['name']
                        sample_pages = int(table['sample_pages'])
                        cur.execute(f"ALTER TABLE {name} STATS_SAMPLE_PAGES = {sample_pages}")
                    con.commit()
            return "COMPLETE", f"Set STATS_SAMPLE_PAGES on {len(tables)} tables"
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def export_procedures(self, request: HandlerRequest):
        data = request.data
        schema = data['schema']
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    for procedure in data['procedures']:
                        print(f'Exporting {schema}.{procedure}')
                        cur.execute(
                            "SELECT COUNT(*) AS total "
                            "FROM information_schema.ROUTINES "
                            "WHERE ROUTINE_SCHEMA = %s "
                            "AND ROUTINE_NAME = %s",
                            (schema, procedure)
                        )
                        count = 0
                        for row in cur.fetchall():
                            count = row['total']
                        if count > 0:
                            cur.execute(f"SHOW CREATE PROCEDURE {schema}.{procedure}")
                            for row in cur.fetchall():
                                text = row['Create Procedure']
                                key = f'procedures/{schema}/{procedure}.txt'
                                body = text.encode('utf-8')
                                md5sum = base64.b64encode(md5(body).digest()).decode('utf-8')
                                self.s3.put_object(
                                    Bucket=self.config['bucket_name'],
                                    Key=key,
                                    ContentMD5=md5sum,
                                    ContentType='text/plain',
                                    Body=body,
                                    ContentLength=len(body)
                                )
                            cur.execute(f"DROP PROCEDURE {schema}.{procedure}")
            return "COMPLETE", 'Exported procedures'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def import_procedures(self, request: HandlerRequest):
        data = request.data
        schema = data['schema']
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    for procedure in data['procedures']:
                        print(f'Importing {schema}.{procedure}')
                        cur.execute(
                            "SELECT COUNT(*) AS total "
                            "FROM information_schema.ROUTINES "
                            "WHERE ROUTINE_SCHEMA = %s "
                            "AND ROUTINE_NAME = %s",
                            (schema, procedure)
                        )
                        count = 0
                        for row in cur.fetchall():
                            count = row['total']
                        if count == 0:
                            key = f'procedures/{schema}/{procedure}.txt'
                            response = self.s3.get_object(
                                Bucket=self.config['bucket_name'],
                                Key=key
                            )
                            text: str = response['Body'].read().decode('utf-8')
                            text = re.sub('CREATE .*? PROCEDURE', 'CREATE PROCEDURE', text)
                            if text.startswith('CREATE PROCEDURE'):
                                cur.execute(text)
            return "COMPLETE", 'Imported procedures'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise
