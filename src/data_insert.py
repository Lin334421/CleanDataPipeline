from clickhouse_driver import Client, connect
import configparser
import os
from loguru import logger

config = configparser.ConfigParser()
# 读取配置文件
current_dir = os.path.dirname(__file__)
# 获取父目录
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
config.read(f'{parent_dir}/config/ck_conn.cfg')  # 假设配置文件名为 config.cfg
data_parents_dir = config['DATA_PATH']['parents_dir']
logger.add('msg.log')

class CKClient:
    def __init__(self, host, port, user, password, database, settings={}, kwargs={}):
        self.client = Client(host=host, port=port, user=user, password=password, database=database, settings=settings,
                             **kwargs)
        self.connect = connect(host=host, port=port, user=user, password=password, database=database)
        self.cursor = self.connect.cursor()

    def execute_with_params(self, sql: object, params: list) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params)
        return result

    def execute_use_setting(self, sql: object, params: list, settings) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params, settings=settings)
        return result

    def execute_no_params(self, sql: object):
        result = self.client.execute(sql)
        return result

    def fetchall(self, sql):
        result = self.client.execute(sql)
        return result

    def close(self):
        self.client.disconnect()


def get_ck_conn_info(port_info):
    conn = config[port_info]
    return {
        "HOST": conn['HOST'],
        "PORT": conn['PORT'],
        "USER": conn['USER'],
        "PASSWD": conn['PASSWD'],
        "DATABASE": conn['DATABASE'],
        "DESCRIPTION": conn['DESCRIPTION']
    }

def get_ck_client(conn_info):
    ck_client = CKClient(host=conn_info['HOST'],
                         port=conn_info['PORT'],
                         database=conn_info['DATABASE'],
                         password=conn_info['PASSWD'],
                         user=conn_info['USER'])

    return ck_client

def insert_into_ck(bulk_data, table_name, file_name=None):
    conn_info = get_ck_conn_info('ClickHouseLocal9000')
    ck_client = get_ck_client(conn_info)
    #
    sql_ = f"INSERT INTO TABLE {table_name} VALUES"
    try:
        count = ck_client.execute_with_params(sql_, bulk_data)
    except Exception as e:
        print(file_name,table_name)
        # with open(f'{parent_dir}/bad_data/bad_data.json','w') as f:
        #     f.write(bulk_data)
        print(bulk_data)
        raise e
    logger.info(f'successfully inserted into ck {table_name} lines count: {count}')
    ck_client.close()
    return count