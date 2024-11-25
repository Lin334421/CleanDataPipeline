import math
from loguru import logger
import os
import re
import shutil
from multiprocessing import Pool
from src.clean_gha_v2 import all_event
from src.config_loader import get_ck_client, ConfigManager
from src.file_cleaner import clean_files, get_already_inserted_files



def list_files(directory):
    gz_file_list = [f for f in
                    os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and f.endswith('.json.gz')]
    json_file_list = set(
        [f for f in os.listdir(directory) if
         os.path.isfile(os.path.join(directory, f)) and re.search(r'\d+\.json$', f)])
    return gz_file_list, json_file_list
# 获取文件列表
# 调度排版task
def schedule_task(json_file_list,num_process=10):
    already_inserted_files = get_already_inserted_files()
    need_parse_files = list(json_file_list - already_inserted_files)
    tasks = []
    logger.info(f"文件个数：{len(need_parse_files)}")
    if len(need_parse_files) < num_process:
        tasks = [[file] for file in need_parse_files]
    else:
        each_process_file_count = len(need_parse_files) // num_process
        for i in range(num_process):
            if i == num_process - 1:
                tasks.append(list(need_parse_files[i * each_process_file_count:]))
            else:
                tasks.append(list(need_parse_files[i * each_process_file_count:(i + 1) * each_process_file_count]))
    return tasks


if __name__ == '__main__':
    num_process = 5
    # 指定目录
    directory = f"{ConfigManager().get_data_parents_dir()}"
    gz_file_list, json_file_list = list_files(directory)
    tasks = schedule_task(json_file_list,num_process)
    print(tasks)
    # with Pool(num_process) as pool:
    #     pool.map(all_event, tasks)
    # 清理已经处理过的文件
    # clean_files(json_file_list, gz_file_list, directory)
