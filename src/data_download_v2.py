import datetime
import os
import random
import sys
import time
import urllib3

from src.data_insert import insert_into_ck
from src.table_name import GHA_DOWNLOAD_INSERT_STATE


# 文件名列表


def download_gha_archive(gha_url, file_parent_path='./',proxy_url=None):
    http = urllib3.PoolManager(num_pools=50)
    failed_urls = []
    req = None
    data_insert_at = int(time.time() * 1000)
    file_name = gha_url.split('/')[-1]
    date_parts = file_name.rstrip('.json.gz').split('-')
    year = int(date_parts[0])
    month = int(date_parts[1])
    day = int(date_parts[2])
    hour = int(date_parts[3])
    archive_date = datetime.datetime(year, month, day, hour)
    file_path = file_parent_path + f'/{file_name}'
    is_successfully_downloaded = False
    if not os.path.exists(file_parent_path):
        os.mkdir(file_parent_path)
    try:
        print(f'download {gha_url}')

        req = http.request('GET', gha_url, preload_content=False, retries=5)
        chunk_size = 1024 * 1024
        is_file_empty = True
        with open(file_path, 'wb') as out:
            b"<?xml version='1.0' encoding='UTF-8'?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>"
            while True:
                data = req.read(chunk_size)
                # try:
                #     decoded_string = data.decode('utf-8')
                # except Exception as e:
                #     decoded_string = ''
                #     raise e
                error_byte_string = b"<?xml version='1.0' encoding='UTF-8'?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>"
                if not data or data == error_byte_string:
                    break
                if random.randint(1, 100) < 1:
                    raise Exception(f'随机失败{file_name}')
                out.write(data)
                is_file_empty = False
        if is_file_empty:
            os.remove(file_path)
        else:
            is_successfully_downloaded = True
            print(f'successfully downloaded {file_name}')

    except urllib3.exceptions.HTTPError as e:
        print('HTTPError::', e)
    except ValueError as e:
        print('failed to handle value', e)
    except Exception as e:
        print(gha_url)
        raise e
    finally:
        if not is_successfully_downloaded:
            if os.path.exists(file_path):
                os.remove(file_path)
            failed_urls.append(gha_url)
        if req:
            req.release_conn()
    bulk_data = [{
        "year": year,
        "month": month,
        "day": day,
        "hour": hour,
        "download_state": 1 if is_successfully_downloaded else 0,
        "unzip_state": 0,
        "insert_state": 0,
        "data_insert_at": data_insert_at
    }
    ]
    insert_into_ck(bulk_data, GHA_DOWNLOAD_INSERT_STATE, file_name)
