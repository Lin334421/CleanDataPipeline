from copy import copy

import time
from multiprocessing import Pool

from src.clean_gha_v2 import all_event
from src.data_insert import get_ck_conn_info, get_ck_client

sql_ = """
select *
from (select year,
             month,
             day,
             hour,
             argMax(download_state, data_insert_at) as download_state,
             argMax(unzip_state, data_insert_at)    as unzip_state,
             argMax(insert_state, data_insert_at)   as insert_state
      from gha_download_insert_state
      group by year, month, day, hour)
where download_state = 1
  and unzip_state = 1
  and insert_state = 0
"""


conn_info = get_ck_conn_info('ClickHouseLocal9000')
ck_client = get_ck_client(conn_info)
results = ck_client.execute_no_params(sql_)

file_state_map = {}
not_insert_file_list = []

# 数据库数据编程映射表
for result in results:
    year = str(result[0])
    month = str(result[1])
    day = str(result[2])
    hour = str(result[3])
    state = str(result[4])
    is_success = result[5]
    # print(year + '_' + month + '_' + day + '_' + hour+'_'+state)

    not_insert_file_list.append((year, month, day, hour))

json_names = []

# 进程数
num_process = 15


for file in not_insert_file_list:
    year = file[0]
    month = file[1]
    day = file[2]
    hour = file[3]
    data_insert_at = int(time.time()*1000)

    if int(month) < 10:
        month = '0' + month
    if int(day) < 10:
        day = '0' + day

    file_path = year + '-' + month + '-' + day + '-'+hour+'.json'
    json_names.append(file_path)

all_event(json_names)
# print(task_list)
# with Pool(num_process) as pool:
#     pool.map(all_event, task_list)