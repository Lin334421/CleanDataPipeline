from clickhouse_driver import Client, connect


# clickhouse_client.py

import clickhouse_driver
from clickhouse_driver import Client, connect

class CKClient:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(CKClient, cls).__new__(cls)
            cls._instance.__init__(*args, **kwargs)
        return cls._instance

    def __init__(self, host, port, user, password, database, settings={}, kwargs={}):
        if not hasattr(self, 'client'):
            self.client = Client(host=host, port=port, user=user, password=password, database=database, settings=settings, **kwargs)
            self.connect = connect(host=host, port=port, user=user, password=password, database=database)
            self.cursor = self.connect.cursor()

    def execute_with_params(self, sql: str, params: list) -> list:
        result = self.client.execute(sql, params)
        return result

    def execute_use_setting(self, sql: str, params: list, settings) -> list:
        result = self.client.execute(sql, params, settings=settings)
        return result

    def execute_no_params(self, sql: str) -> list:
        result = self.client.execute(sql)
        return result

    def fetchall(self, sql: str) -> list:
        result = self.client.execute(sql)
        return result

    def close(self):
        self.client.disconnect()



def get_ck_client(conn_info):
    ck_client = CKClient(host=conn_info['HOST'],
                         port=conn_info['PORT'],
                         database=conn_info['DATABASE'],
                         password=conn_info['PASSWD'],
                         user=conn_info['USER'])

    return ck_client


def get_ck_conn_info(port_info):
    conn = ConfigManager().get_config()[port_info]
    return {
        "HOST": conn['HOST'],
        "PORT": conn['PORT'],
        "USER": conn['USER'],
        "PASSWD": conn['PASSWD'],
        "DATABASE": conn['DATABASE'],
        "DESCRIPTION": conn['DESCRIPTION']
    }