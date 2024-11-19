import time

from src.clean_gha_v2 import insert_into_ck
from src.config_loader import ConfigManager, get_ck_client
from src.table_name import GHA_DOWNLOAD_INSERT_STATE
from utils.un_zip_data import  un_gzip_v2

def unzip_data():
    sql_ = f"""
    select *
    from (select year,
                 month,
                 day,
                 hour,
                 argMax(download_state, data_insert_at) as download_state,
                 argMax(unzip_state, data_insert_at)    as unzip_state,
                 argMax(insert_state, data_insert_at)   as insert_state
          from {GHA_DOWNLOAD_INSERT_STATE}
          group by year, month, day, hour)
    where download_state = 1
      and unzip_state = 0
    """

    ck_client = get_ck_client("ClickHouseLocal9000")
    results = ck_client.execute_no_params(sql_)
    file_state_map = {}
    not_unzip_file_list = []
    # 数据库数据编程映射表
    for result in results:
        year = str(result[0])
        month = str(result[1])
        day = str(result[2])
        hour = str(result[3])
        not_unzip_file_list.append((year, month, day, hour))
    root_path = ConfigManager().get_data_parents_dir()
    for file in not_unzip_file_list:
        year = file[0]
        month = file[1]
        day = file[2]
        hour = file[3]
        data_insert_at = int(time.time()*1000)
        if int(month) < 10:
            month = '0' + month
        if int(day) < 10:
            day = '0' + day
        file_path = root_path +  '/' + year + '-' + month + '-' + day + '-'+hour+'.json.gz'
        is_successful_unzip = un_gzip_v2(f"{year}-{month}-{day}-{hour}.json.gz")
        bulk_data = [{
            "year": int(year),
            "month": int(month),
            "day": int(day),
            "hour": int(hour),
            "download_state": 1,
            "unzip_state": 1 if is_successful_unzip else 0,
            "insert_state": 0,
            "data_insert_at":data_insert_at
        }]
        insert_into_ck(bulk_data, GHA_DOWNLOAD_INSERT_STATE, f'{year}-{month}-{day}-{hour}.json.gz')
# unzip_data()