import base64
from hashlib import md5
import re
from typing import Dict, List

import pymysql
import pymysql.cursors

from bua.sm import SecretManager


class SQL:

    def __init__(self, config, s3, sm: SecretManager):
        self.config = config
        self.s3 = s3
        self.sm = sm
        self.prefix = config['prefix']

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
        con = pymysql.connect(
            host=host, user=username, password=password, database=schema,
            cursorclass=pymysql.cursors.DictCursor, autocommit=False
        )
        return con

    def insert_event_log(self, step, data):
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

    def execute_sql(self, step, data):
        sql = data.get('sql')
        if sql is None:
            sql = self.sm.fetch_secret(data['sqlsecret'])['sql']
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

    def bua_initiate(self, step, data):
        run_type = data['run_type']
        start_inclusive = data.get('start_inclusive')
        end_inclusive = data.get('end_inclusive')
        today = data['today']
        run_date = data['run_date']
        source_date = data.get('source_date')
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    cur.execute(
                        "CALL bua_initiate(%s, %s, %s, %s, %s, %s)",
                        (run_type, start_inclusive, end_inclusive, today, run_date, source_date)
                    )
                    con.commit()
            return "COMPLETE", f'Initiated BUA {run_type} as at {today}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def bua_resolve_variances(self, step, data):
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

    def truncate_workflow_instance(self, step, data):
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

    def get_max_workflow_instance(self, step, data):
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

    def wait_for_workflows(self, step, data):
        workflow_name = data['workflow_name']
        workflow_instance_id = int(data.get('workflow_instance_id', 0))
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
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
                    for row in cur.fetchall():
                        if 'NEW' in row['status']:
                            return "RETRY", f'{row["total"]} workflow instances in NEW status'
                        if 'READY' in row['status']:
                            return "RETRY", f'{row["total"]} workflow instances in READY status'
                        if 'INPROG' in row['status']:
                            return "RETRY", f'{row["total"]} workflow instances in INPROG status'
                        if 'ERROR' in row['status']:
                            return "FAILED", f'{row["total"]} workflow instances in ERROR status'
                        if 'HOLD' in row['status']:
                            return "FAILED", f'{row["total"]} workflow instances in HOLD status'
            return "COMPLETE", f'No workflow instances in NEW/READY/INPROG remain'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def core_warm_database_statistics(self, step, data):
        concurrency = int(data['concurrency'])
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    workflow_instance_id = self._set_max_workflow_instance_id(cur, data)
                    con.commit()
                    cur.execute("CALL core_warm_database_statistics(%s)", (concurrency, ))
                    con.commit()

            return "COMPLETE", f'CORE warm database statistics, max wfi {workflow_instance_id}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def core_warm_database_indexes(self, step, data):
        concurrency = int(data['concurrency'])
        try:
            con = self._connect(data)
            with con:
                cur = con.cursor()
                with cur:
                    workflow_instance_id = self._set_max_workflow_instance_id(cur, data)
                    con.commit()
                    cur.execute("CALL core_warm_database_indexes(%s)", (concurrency, ))
                    con.commit()
            return "COMPLETE", f'CORE warm database indexes, max wfi {workflow_instance_id}'
        except pymysql.err.OperationalError as e:
            if 'timed out' in str(e):
                return "RETRY", f'{e}'
            raise

    def disable_workflow_schedules(self, step, data):
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

    def disable_workflow_instances(self, step, data):
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

    def stats_sample_pages(self, step, data):
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

    def export_procedures(self, step, data):
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

    def import_procedures(self, step, data):
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
