import configparser
import os

from src.ck_client import CKClient

# 创建 ConfigParser 对象
config = configparser.ConfigParser()
# 读取配置文件
current_dir = os.path.dirname(__file__)
# 获取父目录
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
config.read(f'{parent_dir}/config/ck_conn.cfg')  # 假设配置文件名为 config.cfg
data_parents_dir = config['DATA_PATH']['parents_dir']

import configparser
import os


class ConfigManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance.config = configparser.ConfigParser()
            current_dir = os.path.dirname(__file__)
            parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
            config_file_path = f'{parent_dir}/config/ck_conn.cfg'
            print(config_file_path)
            cls._instance.config.read(config_file_path)
            cls._instance.data_parents_dir = cls._instance.config['DATA_PATH']['parents_dir']
        return cls._instance

    def get_config(self):
        return self._instance.config

    def get_data_parents_dir(self):
        return self._instance.data_parents_dir


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


def get_ck_client(port_info):
    conn_info = get_ck_conn_info(port_info)
    return CKClient(
        host=conn_info['HOST'],
        port=int(conn_info['PORT']),
        user=conn_info['USER'],
        password=conn_info['PASSWD'],
        database=conn_info['DATABASE']
    )
