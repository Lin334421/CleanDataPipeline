import calendar
import copy
import datetime
import json
import os
import time
from loguru import logger
from multiprocessing import Pool

from src.config_loader import ConfigManager, get_ck_conn_info, get_ck_client
from src.file_cleaner import clean_file
from src.table_name import GHA_DOWNLOAD_INSERT_STATE, GITHUB_ACTION_EVENTS

"""
服务于大宽表
"""


logger.add('msg.log')








def insert_into_ck(bulk_data, table_name, file_name=None):
    ck_client = get_ck_client('ClickHouseLocal9000')
    #
    sql_ = f"INSERT INTO TABLE {table_name} VALUES"
    try:
        count = ck_client.execute_with_params(sql_, bulk_data)
    except Exception as e:
        print(file_name, table_name)
        # with open(f'{parent_dir}/bad_data/bad_data.json','w') as f:
        #     f.write(bulk_data)
        # print(bulk_data)
        raise e
    logger.info(f'successfully inserted into ck {table_name} lines count: {count}')
    ck_client.close()
    return count


def get_index_name(index_name):
    result = index_name[0].lower()
    for i in range(1, len(index_name)):
        if index_name[i].isupper():
            result = result + '_' + index_name[i].lower()
        else:
            result = result + index_name[i]
    return result


with open('gha_tplt.json', 'r') as f:
    event_tmpls = json.load(f)
default_type = {}
with open('gha_final_default_type.json', 'r') as f:
    json_data = json.load(f)
    for key, v in json_data.items():
        if key != 'search_key_update_at' and key.endswith('_at'):
            default_type[key] = datetime.datetime(1970, 1, 1, 0, 0)
        else:
            default_type[key] = v


event_counter = {}

def all_event(file_names):
    # parents_dir = '/home/malin'
    updated_at = int(time.time() * 1000)

    def get_event_template(event_type):
        return event_tmpls.get(event_type,None)
    for file_name in file_names:
        with open(f'{ConfigManager().get_data_parents_dir()}/{file_name}', 'r') as f:
            logger.info(f'开始解析gha ghaname:{file_name}................')
            gh_archive_year = file_name.split('-')[0]
            gh_archive_month = file_name.split('-')[1]
            gh_archive_day = file_name.split('-')[2]
            gh_archive_hour = file_name.split('-')[3][0:-5]
            # logger.info(event_tplt)
            lines = f.readlines()
            count = 0
            # 加载每一条数据
            bulk_data = []
            # error
            for line in lines:
                try:
                    event_data = json.loads(line)
                except:
                    logger.info(f'{file_name} line json.decoder.JSONDecodeError')
                    continue
                need_insert_event_data = copy.deepcopy(default_type)
                # 如将 PushEvent 修改成 push_event
                event_type = get_index_name(event_data['type'])
                event_counter[event_type] = event_counter.get(event_type, 0) + 1

                owner = event_data['repo']['name'].split('/')[0]
                repo = event_data['repo']['name'].split('/')[1]
                id = event_data['id']
                need_insert_event_data['search_key_event_type'] = event_type
                need_insert_event_data['search_key_gh_archive_year'] = gh_archive_year
                need_insert_event_data['search_key_gh_archive_month'] = gh_archive_month
                need_insert_event_data['search_key_gh_archive_hour'] = gh_archive_hour
                need_insert_event_data['search_key_gh_archive_day'] = gh_archive_day
                need_insert_event_data['search_key_updated_at'] = updated_at
                need_insert_event_data['search_key_owner'] = owner
                need_insert_event_data['search_key_repo'] = repo
                need_insert_event_data['search_key_id'] = id
                need_insert_event_data['search_key_updated_at'] = updated_at

                count += 1
                # debug 区
                # logger.info(event_data)
                # logger.info(json.dumps(event_data, indent=4))
                # with open('../output/event_data.json', 'w') as f:
                #     json.dumps(event_data, indent=4)
                # if event_type in (
                # 'pull_request_event', 'issues_event', 'issue_comment_event', 'pull_request_review_comment_event'):

                event_tplt = get_event_template(event_type)
                if not event_tplt:
                    logger.info(f'{event_type} not found')
                    continue
                # print(line)
                # print(event_type)
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
                            try:
                                eval_data = eval(event_tplt[event])
                            except:
                                eval_data = None

                            if event_type == 'push_event' and event == 'payload__commits':
                                commits = eval_data
                                shas = []
                                author__emails = []
                                author__names = []
                                messages = []
                                for commit in commits:
                                    shas.append(commit['sha'])
                                    if commit['author']['email'] is None:
                                        author__emails.append('')
                                    else:
                                        author__emails.append(commit['author']['email'])
                                    if commit['author']['name'] is None:
                                        author__names.append('')
                                    else:
                                        author__names.append(commit['author']['name'])
                                    messages.append(commit['message'])
                                need_insert_event_data['payload__commits.author__email'] = author__emails
                                need_insert_event_data['payload__commits.author__name'] = author__names
                                need_insert_event_data['payload__commits.sha'] = shas
                                need_insert_event_data['payload__commits.message'] = messages
                            elif event_type in (
                            'pull_request_event', 'pull_request_review_comment_event', 'issue_comment_event',
                            'issues_event') and event in (
                            'payload__pull_request__assignees', 'payload__issue__assignees'):
                                assignees = eval_data
                                logins = []
                                ids = []
                                node_ids = []
                                types = []
                                if assignees:
                                    for assignee in assignees:
                                        logins.append(assignee['login'])
                                        ids.append(assignee['id'])
                                        node_ids.append(assignee['node_id'])
                                        types.append(assignee['type'])
                                else:
                                    logins = ['']
                                    ids = [0]
                                    node_ids = ['']
                                    types = ['']
                                need_insert_event_data[f'{event}.login'] = logins
                                need_insert_event_data[f'{event}.id'] = ids
                                need_insert_event_data[f'{event}.node_id'] = node_ids
                                need_insert_event_data[f'{event}.type'] = types
                            elif event_type == 'gollum_event' and event == 'payload__pages':
                                pages = eval_data
                                page_names = []
                                titles = []
                                summaries = []
                                actions = []
                                shas = []
                                if pages:
                                    for page in pages:
                                        page_names.append(page['page_name'])
                                        titles.append(page['title'])
                                        summaries.append(page['summary'] if page['summary'] else '')
                                        actions.append(page['action'])
                                        shas.append(page['sha'])
                                else:
                                    page_names = ['']
                                    titles = ['']
                                    summaries = ['']
                                    actions = ['']
                                    shas = ['']
                                need_insert_event_data['payload__pages.page_name'] = page_names
                                need_insert_event_data['payload__pages.title'] = titles
                                need_insert_event_data['payload__pages.summary'] = summaries
                                need_insert_event_data['payload__pages.action'] = actions
                                need_insert_event_data['payload__pages.sha'] = shas
                            else:
                                if eval_data:
                                    need_insert_event_data[event] = eval_data
                        except:
                            need_insert_event_data[event] = ''
                            # logger.info(event_tplt[event])
                            # raise Exception

                # logger.info(need_insert_event_data)

                bulk_data.append(need_insert_event_data)
                # if count %100==0:
                #     try:
                #         insert_into_ck(bulk_data, 'gha_event_beta1')
                #     except:
                #         print(bulk_data)
                #         break
                #     bulk_data.clear()
                # if count == 1000:
                #     break

            logger.info(f'文件{file_name} 总解析行数:{count}')
            # print(bulk_data)
            count = insert_into_ck(bulk_data, GITHUB_ACTION_EVENTS)
            bulk_data.clear()
            if count != 0:
                bulk_state_data = [{
                    "year": int(gh_archive_year),
                    "month": int(gh_archive_month),
                    "day": int(gh_archive_day),
                    "hour": int(gh_archive_hour),
                    "download_state": 1,
                    "unzip_state": 1,
                    "insert_state": 1,
                    "data_insert_at": updated_at
                }]
                insert_into_ck(bulk_state_data, GHA_DOWNLOAD_INSERT_STATE, f'{int(gh_archive_year)}-{int(gh_archive_month)}-{int(gh_archive_day)}-{int(gh_archive_hour)}.json.gz')
        # clean_file(f'{ConfigManager().get_data_parents_dir()}',file_name)




