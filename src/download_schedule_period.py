import schedule
import time

from src.clean_gha_v2 import all_event
from src.config_loader import ConfigManager
from src.listfile import schedule_task, list_files
from src.update import download_and_unzip

from multiprocessing import Pool

def my_task():
    num_process = 5
    # 指定目录
    directory = f"{ConfigManager().get_data_parents_dir()}"
    gz_file_list, json_file_list = list_files(directory)
    tasks = schedule_task(json_file_list, num_process)
    print(tasks)
    with Pool(num_process) as pool:
        pool.map(all_event, tasks)

# 每 60 秒执行一次
schedule.every(10).seconds.do(download_and_unzip)




# 开始循环
while True:
    schedule.run_pending()  # 执行所有到期任务
    time.sleep(2)

# task_func()
