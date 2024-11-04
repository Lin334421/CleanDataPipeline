import datetime
import os
import random
import sys
import time

import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.clean_gha import get_ck_conn_info, get_ck_client, insert_into_ck

# 文件名列表
url_array = []

# sample url: https://data.gharchive.org/2022-01-01-1.json.gz

# year = int(sys.argv[1])
year = 2024
current_date = datetime.datetime(year, 10, 8, 6)
until_date = datetime.datetime.utcnow()-datetime.timedelta(hours=1)
until_date = until_date.replace(minute=0, second=0, microsecond=0)

print(f'{until_date}')
# until_date = datetime.datetime(year, 12, 31, 23)

if not os.path.exists(f'./{year}'):
    os.mkdir(f'./{year}')

current_date_str = current_date.strftime('%Y-%m-%d-%-H')
download_url = f'https://data.gharchive.org/{current_date_str}.json.gz'
url_array.append(download_url)

while current_date < until_date:
    current_date = current_date + datetime.timedelta(hours=1)
    current_date_str = current_date.strftime('%Y-%m-%d-%-H')
    download_url = f'https://data.gharchive.org/{current_date_str}.json.gz'
    url_array.append(download_url)

print(url_array)
failed_urls = []
http = urllib3.PoolManager(num_pools=50)


def download_gha_archive(gha_url, file_parent_path='./'):
    global year
    req = None
    data_insert_at = int(time.time()*1000)
    file_name = gha_url.split('/')[-1]
    date_parts = file_name.rstrip('.json.gz').split('-')
    year = int(date_parts[0])
    month = int(date_parts[1])
    day = int(date_parts[2])
    hour = int(date_parts[3])
    archive_date = datetime.datetime(year, month, day, hour)
    file_path = file_parent_path + f'/{year}/{file_name}'
    is_successfully_downloaded = False
    if not os.path.exists(file_parent_path + f'/{year}'):
        os.mkdir(file_parent_path + f'/{year}')
    try:
        print(f'download {gha_url}')
        req = http.request('GET', gha_url, preload_content=False, retries=3)
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
                if random.randint(1,100) < 1:
                    raise Exception(f'随机失败{file_name}')
                out.write(data)
                is_file_empty = False
        if is_file_empty:
            os.remove(file_path)
        else:
            is_successfully_downloaded = True

    except urllib3.exceptions.HTTPError as e:
        print('HTTPError::', e)
    except ValueError as e:
        print('failed to handle value', e)
    except Exception as e:
        print(e)
    finally:
        if not is_successfully_downloaded:
            if os.path.exists(file_path):
                os.remove(file_path)
            failed_urls.append(gha_url)
        if req:
            req.release_conn()
    # print(is_successfully_downloaded)
    bulk_data = [{
            "year": year,
            "month": month,
            "day": day,
            "hour": hour,
            "state": "download",
            "is_success": 1 if is_successfully_downloaded else 0,
            "data_insert_at":data_insert_at
        },{
            "year": year,
            "month": month,
            "day": day,
            "hour": hour,
            "state": "unzip",
            "is_success": 0,
            "data_insert_at":data_insert_at
        },
        {
            "year": year,
            "month": month,
            "day": day,
            "hour": hour,
            "state": "insert",
            "is_success": 0,
            "data_insert_at": data_insert_at
        }
    ]
    insert_into_ck(bulk_data, 'gha_download_insert_state', file_name)

root_path = '/Users/jonas/PycharmProjects/CleanDataPipeline/download_data'

# conn_info = get_ck_conn_info('ClickHouseLocal9000')
# ck_client = get_ck_client(conn_info)
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = []
    for download_url in url_array:
        futures.append(executor.submit(download_gha_archive, download_url,root_path))
    for future in futures:
        future.result()


# for gha_url in url_array:
#     req = None



with open('failed_urls.txt', 'w') as wf:
    wf.write('\n'.join(failed_urls))
