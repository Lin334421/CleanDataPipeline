import datetime
import os
import time
from loguru import logger
from src.clean_gha_v2 import all_event
from src.config_loader import get_ck_client, ConfigManager
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
from src.data_download_v2 import download_gha_archive
from src.listfile import list_files, schedule_task
from src.table_name import GITHUB_ACTION_EVENTS
from src.unzip_process import unzip_data
from utils.un_zip_data import un_gzip_v2


# 测试表github_action_events_beta


def get_last_update_time():
    sql_ = f"""
    select max(created_at) as max_date,
       toYear(max_date) as year,
       toMonth(max_date) as month,
       toDayOfMonth(max_date) as day,
       toHour(max_date) as hour
    from {GITHUB_ACTION_EVENTS}
    """
    ck_client = get_ck_client('ClickHouseLocal9000')
    results = ck_client.execute_no_params(sql_)
    # print(results[0][1])
    year = results[0][1]
    month = results[0][2]
    day = results[0][3]
    hour = results[0][4]
    return year,month,day,hour

def download_and_unzip():
    logger.info(f"更新任务")
    root_path = ConfigManager().get_data_parents_dir()

    year, month, day, hour = get_last_update_time()
    current_date = datetime.datetime(year, month, day, hour) + datetime.timedelta(hours=1)
    until_date = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
    until_date = until_date.replace(minute=0, second=0, microsecond=0)
    url_array = []
    gz_file_list, _ = list_files(root_path)

    while current_date < until_date:
        current_date_str = current_date.strftime('%Y-%m-%d-%-H')
        # print(gz_file_list)
        download_url = f'https://data.gharchive.org/{current_date_str}.json.gz'
        if current_date_str + '.json.gz' not in gz_file_list:
            # print(current_date_str)
            url_array.append(download_url)
        current_date = current_date + datetime.timedelta(hours=1)
    print(f"总需要更新的文件个数{len(url_array)}")

    url_array = url_array[:1]
    print(url_array)
    print(f"当前需要更新的文件个数{len(url_array)}")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for download_url in url_array:
            futures.append(executor.submit(download_gha_archive, download_url, root_path))
        for future in futures:
            future.result()
    time.sleep(2)
    # 解压
    unzip_data()
    num_process = 5
    # 指定目录
    directory = f"{ConfigManager().get_data_parents_dir()}"
    gz_file_list, json_file_list = list_files(directory)
    need_unzip_files = [file for file in gz_file_list if file[:-3] not in json_file_list]
    for file in need_unzip_files:
        un_gzip_v2(file)
    time.sleep(2)
    # 多进程插入
    tasks = schedule_task(json_file_list)

    # with ThreadPoolExecutor(max_workers=5) as executor:
    #     futures = []
    #     for task in tasks:
    #         futures.append(executor.submit(all_event,task))
    #     for future in futures:
    #         future.result()
    # logger.info("任务执行完成")
    all_event(tasks[0])

# 下载


# 解压


# 插入


# 清理




