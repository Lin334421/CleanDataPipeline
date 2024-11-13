import configparser
import gzip
import calendar
import os
import time
from loguru import logger
logger.add('un_zip.log')
# 创建 ConfigParser 对象
config = configparser.ConfigParser()
# 读取配置文件
current_dir = os.path.dirname(__file__)
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
config.read(f'{parent_dir}/config/ck_conn.cfg')  # 假设配置文件名为 config.cfg
data_parents_dir = config['DATA_PATH']['parents_dir']
if not data_parents_dir.endswith('/'):
    data_parents_dir+='/'
def un_gzip(gz_filename):
    try:
        filename = gz_filename.replace('.gz', '')
        g_file = gzip.GzipFile(f'{data_parents_dir}{gz_filename}')
        open(f'{data_parents_dir}{filename}', 'wb+').write(g_file.read())
        g_file.close()
        logger.info(f"successful unzip {gz_filename}")
    except Exception as e:
        logger.error(e)
        logger.error(f"unzip {gz_filename} failed")


def un_gzip_v2(gz_filename):
    global data_parents_dir
    year = gz_filename.split('-')[0]
    parents_dir = data_parents_dir+year+'/'
    is_successful_unzip = False
    try:
        filename = gz_filename.replace('.gz', '')
        g_file = gzip.GzipFile(f'{parents_dir}{gz_filename}')
        open(f'{parents_dir}{filename}', 'wb+').write(g_file.read())
        g_file.close()
        logger.info(f"successful unzip {gz_filename}")
        is_successful_unzip =True
    except Exception as e:
        logger.error(e)
        logger.error(f"unzip {gz_filename} failed")
    return is_successful_unzip


# for year in year_list:
#     count = 0
#     for month in month_list:
#         day_count = calendar.monthrange(year, month)[1]
#         if month < 10:
#             month = '0' + str(month)
#         for i in range(1, day_count + 1):
#             if i < 10:
#                 i = '0' + str(i)
#             for j in range(24):
#                 un_gzip(f"{year}-{month}-{i}-{j}.json.gz")
#                 count+=1

