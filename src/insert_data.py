from copy import copy

from src.clean_gha import get_ck_conn_info, get_ck_client, insert_into_ck, all_event
import time
from multiprocessing import Pool
sql_ = """
select year,month,day,hour,state,argMax(is_success,data_insert_at)
from gha_download_insert_state
where state = 'download'
   group by year,month,day,hour,state
union all
select year,month,day,hour,state,argMax(is_success,data_insert_at)
from gha_download_insert_state
where state = 'unzip'
   group by year,month,day,hour,state
union all
select year,month,day,hour,state,argMax(is_success, data_insert_at)
from gha_download_insert_state
where state = 'insert' group by year,month,day,hour,state
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
    file_state_map[year + '_' + month + '_' + day + '_' + hour + '_' + state] = is_success
    if state == 'insert' and is_success == 0:
        not_insert_file_list.append((year, month, day, hour))

root_path = '/Users/jonas/PycharmProjects/CleanDataPipeline/download_data'
json_names = []

# 进程数
num_process = 15


for file in not_insert_file_list:
    year = file[0]
    month = file[1]
    day = file[2]
    hour = file[3]
    data_insert_at = int(time.time()*1000)

    # print(year + '_' + month + '_' + day + '_' + hour + '_' + 'download')
    if file_state_map.get(year + '_' + month + '_' + day + '_' + hour + '_' + 'download') == 1 and file_state_map.get(
            year + '_' + month + '_' + day + '_' + hour + '_' + 'unzip') == 1:
        if int(month) < 10:
            month = '0' + month
        if int(day) < 10:
            day = '0' + day

        file_path = year + '-' + month + '-' + day + '-'+hour+'.json'
        json_names.append(file_path)
        # bulk_data = [{
        #     "year": int(year),
        #     "month": int(month),
        #     "day": int(day),
        #     "hour": int(hour),
        #     "state": "insert",
        #     "is_success": 1,
        #     "data_insert_at": data_insert_at
        # }]
        # insert_into_ck(bulk_data, 'gha_download_insert_state', f'{year}-{month}-{day}-{hour}.json.gz')
# task_list = []
# for i in json_names:
#     task_list.append([i])
all_event(json_names)
# print(task_list)
# with Pool(num_process) as pool:
#     pool.map(all_event, task_list)