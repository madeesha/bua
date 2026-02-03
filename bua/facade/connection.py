from typing import Optional, Dict

from pymysql import Connection
from pymysql.cursors import SSDictCursor


class DBProxy:
    def __init__(self, mysql, username, password, lock_wait_timeout=60):
        self.mysql = mysql
        self.username = username
        self.password = password
        self.lock_wait_timeout = lock_wait_timeout
        self.host: Optional[str] = None
        self.conn: Optional[Connection] = None

    def connect(self, event: Dict):
        prefix = event['prefix']
        update_id = event['update_id']
        suffix = event['suffix']
        domain = event['domain']
        schema = event['schema']
        try:
            return self._connect(prefix, update_id, suffix, domain, schema)
        except:
            self.disconnect()
            return self._connect(prefix, update_id, suffix, domain, schema)

    def _connect(self, prefix, update_id, suffix, domain, schema):
        # Determine the host based on whether this is Aurora or RDS
        # Aurora cluster endpoints contain '.cluster-' or '.cluster.'
        # Also, if update_id is empty, assume we're using a direct cluster endpoint
        if '.cluster-' in domain or '.cluster.' in domain:
            # Aurora cluster endpoint already formatted in domain
            host = f'{prefix}.{domain}'
        elif update_id == '' or update_id is None:
            # Direct hostname provided (Aurora cluster endpoint)
            host = f'{prefix}.{domain}'
        else:
            # Traditional RDS instance identifier
            db_instance_identifier = f'{prefix}-{update_id}-{suffix}'
            host = f'{db_instance_identifier}.{domain}'
        
        if self.host is None or self.host != host or self.conn is None:
            self.conn = self.mysql.connect(
                host=host, user=self.username, password=self.password, database=schema,
                cursorclass=self.mysql.cursors.SSDictCursor, autocommit=False
            )
        with self.conn.cursor() as cur:
            cur.execute(f"SET SESSION innodb_lock_wait_timeout = {self.lock_wait_timeout}")
        self.host = host

    def disconnect(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception as ex:
                pass
            self.conn = None

    def cursor(self) -> SSDictCursor:
        if self.conn is None:
            raise RuntimeError('Not connected')
        return self.conn.cursor()

    def commit(self):
        if self.conn is None:
            raise RuntimeError('Not connected')
        self.conn.commit()

    def rollback(self):
        if self.conn is None:
            raise RuntimeError('Not connected')
        self.conn.rollback()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.disconnect()
        return False