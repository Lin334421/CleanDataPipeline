import calendar
import datetime
import json
import os
import time
from loguru import logger
from multiprocessing import Pool
from clickhouse_driver import Client, connect

import configparser

from tmpl.gha_event import event_tmpls

# 创建 ConfigParser 对象
config = configparser.ConfigParser()
# 读取配置文件
current_dir = os.path.dirname(__file__)
# 获取父目录
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
config.read(f'{parent_dir}/config/ck_conn.cfg')  # 假设配置文件名为 config.cfg
logger.add('msg.log')


class CKClient:
    def __init__(self, host, port, user, password, database, settings={}, kwargs={}):
        self.client = Client(host=host, port=port, user=user, password=password, database=database, settings=settings,
                             **kwargs)
        self.connect = connect(host=host, port=port, user=user, password=password, database=database)
        self.cursor = self.connect.cursor()

    def execute_with_params(self, sql: object, params: list) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params)
        return result

    def execute_use_setting(self, sql: object, params: list, settings) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params, settings=settings)
        return result

    def execute_no_params(self, sql: object):
        result = self.client.execute(sql)
        return result

    def fetchall(self, sql):
        result = self.client.execute(sql)
        return result

    def close(self):
        self.client.disconnect()


def get_ck_client(conn_info):
    ck_client = CKClient(host=conn_info['HOST'],
                         port=conn_info['PORT'],
                         database=conn_info['DATABASE'],
                         password=conn_info['PASSWD'],
                         user=conn_info['USER'])

    return ck_client


def get_ck_conn_info(port_info):
    conn = config[port_info]
    return {
        "HOST": conn['HOST'],
        "PORT": conn['PORT'],
        "USER": conn['USER'],
        "PASSWD": conn['PASSWD'],
        "DATABASE": conn['DATABASE'],
        "DESCRIPTION": conn['DESCRIPTION']
    }


def insert_into_ck(bulk_data, table_name):
    conn_info = get_ck_conn_info('ClickHouseLocal9000')
    ck_client = get_ck_client(conn_info)
    #
    sql_ = f"INSERT INTO TABLE {table_name} VALUES"
    try:
        count = ck_client.execute_with_params(sql_, bulk_data)
    except Exception as e:
        with open(f'{parent_dir}/bad_data/bad_data.json') as f:
            json.dump(bulk_data, f)
        raise e
    logger.info(f'successfully inserted into ck {table_name} lines count: {count}')
    ck_client.close()


def get_index_name(index_name):
    result = index_name[0].lower()
    for i in range(1, len(index_name)):
        if index_name[i].isupper():
            result = result + '_' + index_name[i].lower()
        else:
            result = result + index_name[i]
    return result


def all_event(file_names):
    parents_dir = '/opt/mission_area/gha_data'
    # parents_dir = '/home/malin'
    updated_at = int(time.time() * 1000)
    bulk_datas = {
        'fork_event': [],
        'pull_request_event': [],
        'pull_request_review_event': [],
        'push_event': [],
        'issues_event': [],
        'watch_event': []
    }
    pr_event_tplt = event_tmpls.get('pull_request_event')
    fork_event_tplt = event_tmpls.get('fork_event')
    pr_review_event_tplt = event_tmpls.get('pull_request_review_event')
    push_event_tplt = event_tmpls.get('push_event')
    issues_event_tplt = event_tmpls.get('issues_event')
    watch_event_tplt = event_tmpls.get('watch_event')
    for file_name in file_names:
        with open(f'{parents_dir}/{file_name}', 'r') as f:
            logger.info(f'开始解析gha ghaname:{file_name}................')
            gh_archive_year = file_name.split('-')[0]
            gh_archive_month = file_name.split('-')[1]
            gh_archive_day = file_name.split('-')[2]
            gh_archive_hour = file_name.split('-')[3][0:-5]
            # logger.info(event_tplt)
            lines = f.readlines()
            count = 0
            # 加载每一条数据
            for line in lines:
                try:
                    event_data = json.loads(line)
                except:
                    logger.info(f'{file_name} line json.decoder.JSONDecodeError')
                    continue
                need_insert_event_data = {}
                # 如将 PushEvent 修改成 push_event
                event_type = get_index_name(event_data['type'])
                # if event_type != 'pull_request_event' \
                #         and event_type != 'fork_event' \
                #         and event_type != 'pull_request_review_event' \
                #         and event_type != 'push_event' \
                #         and event_type !='issues_event' :
                if event_type != 'push_event' and event_type != 'pull_request_event':
                    # logger.info(event_type)
                    continue
                owner = event_data['repo']['name'].split('/')[0]
                repo = event_data['repo']['name'].split('/')[1]
                id = int(event_data['id'])
                need_insert_event_data['search_key__event_type'] = event_type
                need_insert_event_data['search_key__gh_archive_year'] = gh_archive_year
                need_insert_event_data['search_key__gh_archive_month'] = gh_archive_month
                need_insert_event_data['search_key__gh_archive_hour'] = gh_archive_hour
                need_insert_event_data['search_key__gh_archive_day'] = gh_archive_day
                need_insert_event_data['search_key__updated_at'] = updated_at
                need_insert_event_data['search_key__owner'] = owner
                need_insert_event_data['search_key__repo'] = repo
                need_insert_event_data['search_key__id'] = id
                count += 1
                # debug 区
                # logger.info(event_data)
                # logger.info(json.dumps(event_data, indent=4))
                # with open('../output/event_data.json', 'w') as f:
                #     json.dumps(event_data, indent=4)
                if event_type == 'pull_request_event':
                    event_tplt = pr_event_tplt
                    bulk_data = bulk_datas.get('pull_request_event')
                    table_name = 'cleaned_mini_pull_request_event'

                elif event_type == 'fork_event':
                    event_tplt = fork_event_tplt
                    bulk_data = bulk_datas.get('fork_event')
                    table_name = 'fork_event_simple'
                elif event_type == 'pull_request_review_event':
                    event_tplt = pr_review_event_tplt
                    bulk_data = bulk_datas.get('pull_request_review_event')
                    table_name = 'cleaned_mini_pull_request_review_event'
                elif event_type == 'push_event':
                    event_tplt = push_event_tplt
                    bulk_data = bulk_datas.get('push_event')
                    table_name = 'cleaned_mini_push_event_v2'
                elif event_type == 'issues_event':
                    event_tplt = issues_event_tplt
                    bulk_data = bulk_datas.get('issues_event')
                    table_name = 'cleaned_mini_issues_event'
                elif event_type == 'watch_event':
                    event_tplt = watch_event_tplt
                    bulk_data = bulk_datas.get('watch_event')
                    table_name = 'cleaned_mini_watch_event'
                for event in event_tplt:
                    if event.endswith('_at'):
                        try:
                            eval(event_tplt[event])
                        except:
                            # logger.info('错误')
                            need_insert_event_data[event] = datetime.datetime.strptime('1970-01-01T00:00:00Z',
                                                                                       '%Y-%m-%dT%H:%M:%SZ')
                            continue

                        if not eval(event_tplt[event]):
                            need_insert_event_data[event] = datetime.datetime.strptime('1970-01-01T00:00:00Z',
                                                                                       '%Y-%m-%dT%H:%M:%SZ')
                        else:
                            need_insert_event_data[event] = datetime.datetime.strptime(eval(event_tplt[event]),
                                                                                       '%Y-%m-%dT%H:%M:%SZ')
                    else:
                        try:
                            if event_type == 'push_event' and event == 'payload__commits':
                                commits = eval(event_tplt[event])
                                shas = []
                                author__emails = []
                                author__names = []
                                messages = []
                                distincts = []
                                urls = []
                                for commit in commits:
                                    shas.append(commit['sha'])
                                    author__emails.append(commit['author']['email'])
                                    author__names.append(commit['author']['name'])
                                    messages.append(commit['message'])
                                    distincts.append(str(commit['distinct']))
                                    urls.append(commit['url'])
                                need_insert_event_data['payload__commits.author__email'] = author__emails
                                need_insert_event_data['payload__commits.author__name'] = author__names
                                need_insert_event_data['payload__commits.sha'] = shas
                                need_insert_event_data['payload__commits.url'] = urls
                                need_insert_event_data['payload__commits.distinct'] = distincts
                                need_insert_event_data['payload__commits.message'] = messages

                            else:

                                need_insert_event_data[event] = str(eval(event_tplt[event]))
                        except:
                            need_insert_event_data[event] = ''
                            # logger.info(event_tplt[event])
                            # raise Exception

                # logger.info(need_insert_event_data)

                bulk_data.append(need_insert_event_data)
                if len(bulk_data) >= 50000:
                    insert_into_ck(bulk_data, table_name)
                    # 避免持续占用内存,而不释放
                    bulk_data.clear()
                # return
            logger.info(f'文件{file_name} 总解析行数:{count}')

    # 插入数据库
    pr_bulk_data = bulk_datas.get('pull_request_event')
    fork_bulk_data = bulk_datas.get('fork_event')
    pr_review_bulk_data = bulk_datas.get('pull_request_review_event')
    push_bulk_data = bulk_datas.get('push_event')
    watch_bulk_data = bulk_datas.get('watch_event')
    # logger.info(watch_bulk_data)

    if pr_bulk_data:
        insert_into_ck(pr_bulk_data, 'cleaned_mini_pull_request_event')
    # if fork_bulk_data:
    #     insert_into_ck(fork_bulk_data, 'fork_event_simple')
    # if pr_review_bulk_data:
    #     insert_into_ck(pr_review_bulk_data, 'cleaned_mini_pull_request_review_event')
    if push_bulk_data:
        insert_into_ck(push_bulk_data, 'cleaned_mini_push_event_v2')
    # if issues_event_tplt:
    #     insert_into_ck(issues_event_tplt, 'cleaned_mini_issues_event')
    # if watch_bulk_data:
    #     insert_into_ck(watch_bulk_data, 'cleaned_mini_watch_event')


if __name__ == '__main__':
    json_names = []

    years = config['DATA_TIME']['year'].split(',')
    months = config['DATA_TIME']['month'].split(',')
    # for hour in range(24):
    #     json_names.append(f'2021-01-01-{hour}.json')
    for year in years:
        year = int(year)
        for month in months:
            month = int(month)
            day_count = calendar.monthrange(year, month)[1]
            if month < 10:
                month = '0' + str(month)
            for day in range(1, day_count + 1):
                if day < 10:
                    day = '0' + str(day)
                one_day_file_name = []
                for hour in range(24):
                    logger.info(f'{year}-{month}-{day}-{hour}.json')
                    one_day_file_name.append(f'{year}-{month}-{day}-{hour}.json')
                    # logger.info(f'{year}-{month}-{day}-{hour}.json')
                json_names.append(one_day_file_name)

# # json_names=[['2022-07-01-3.json']]


# with Pool(15) as pool:
#     pool.map(all_event, json_names)
