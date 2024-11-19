import os

from src.config_loader import get_ck_client
from src.table_name import GHA_DOWNLOAD_INSERT_STATE


def get_already_inserted_files():
    already_inserted_files = set()
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
    where insert_state = 1
        """
    ck_client = get_ck_client('ClickHouseLocal9000')
    results = ck_client.execute_no_params(sql_)
    for result in results:
        year = str(result[0])
        month = str(result[1] if result[1] > 9 else '0' + str(result[1]))
        day = str(result[2] if result[2] > 9 else '0' + str(result[2]))
        hour = str(result[3])
        already_inserted_files.add(year + '-' + month + '-' + day + '-' + hour + '.json')
    ck_client.close()
    return already_inserted_files


def clean_file(parent_path, file_name):
    already_inserted_files = get_already_inserted_files()
    if file_name in already_inserted_files:
        os.remove(parent_path + '/' + file_name)
        os.remove(parent_path + '/' + file_name + '.gz')
        print(f'清理文件 {file_name}')


def clean_files(json_file_list, gz_file_list, directory):
    already_inserted_files = get_already_inserted_files()

    for file in json_file_list:
        if file in already_inserted_files:
            # shutil.rmtree(directory+'/'+file)
            os.remove(directory + '/' + file)
            print(directory + '/' + file)
    for file in gz_file_list:
        if file[:-3] in already_inserted_files:
            os.remove(directory + '/' + file)
            print(directory + '/' + file)
