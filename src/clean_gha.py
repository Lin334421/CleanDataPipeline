import calendar
import datetime
import json
import os
import time
from loguru import logger
from multiprocessing import Pool
from clickhouse_driver import Client, connect

import configparser

event_tmpls = {
    'pull_request_event': {'id': "event_data['id']", 'type': "event_data['type']",
                           'created_at': "event_data['created_at']", 'actor__id': "event_data['actor']['id']",
                           'actor__login': "event_data['actor']['login']",
                           'actor__display_login': "event_data['actor']['display_login']",
                           'repo__id': "event_data['repo']['id']", 'repo__name': "event_data['repo']['name']",
                           'payload__action': "event_data['payload']['action']",
                           'payload__number': "event_data['payload']['number']",
                           'payload__pull_request__id': "event_data['payload']['pull_request']['id']",
                           'payload__pull_request__node_id': "event_data['payload']['pull_request']['node_id']",
                           'payload__pull_request__number': "event_data['payload']['pull_request']['number']",
                           'payload__pull_request__state': "event_data['payload']['pull_request']['state']",
                           'payload__pull_request__title': "event_data['payload']['pull_request']['title']",
                           'payload__pull_request__user__login': "event_data['payload']['pull_request']['user']['login']",
                           'payload__pull_request__user__id': "event_data['payload']['pull_request']['user']['id']",
                           'payload__pull_request__user__node_id': "event_data['payload']['pull_request']['user']['node_id']",
                           'payload__pull_request__user__type': "event_data['payload']['pull_request']['user']['type']",
                           'payload__pull_request__created_at': "event_data['payload']['pull_request']['created_at']",
                           'payload__pull_request__updated_at': "event_data['payload']['pull_request']['updated_at']",
                           'payload__pull_request__base__repo__fork': "event_data['payload']['pull_request']['base']['repo']['fork']",
                           'payload__pull_request__merged': "event_data['payload']['pull_request']['merged']",
                           'payload__pull_request__body': "event_data['payload']['pull_request']['body']",
                           'payload__pull_request__merge_commit_sha': "event_data['payload']['pull_request']['merge_commit_sha']",
                           'payload__pull_request__head__repo__private': "event_data['payload']['pull_request']['head']['repo']['private']",
                           'payload__pull_request__head__repo__fork': "event_data['payload']['pull_request']['head']['repo']['fork']",
                           'payload__pull_request__base__repo__private': "event_data['payload']['pull_request']['base']['repo']['private']",
                           'payload__pull_request__closed_at': "event_data['payload']['pull_request']['closed_at']",
                           'payload__pull_request__merged_at': "event_data['payload']['pull_request']['merged_at']",
                           'payload__pull_request__head__label': "event_data['payload']['pull_request']['head']['label']",
                           'payload__pull_request__head__ref': "event_data['payload']['pull_request']['head']['ref']",
                           'payload__pull_request__head__sha': "event_data['payload']['pull_request']['head']['sha']",
                           'payload__pull_request__head__user__login': "event_data['payload']['pull_request']['head']['user']['login']",
                           'payload__pull_request__head__user__id': "event_data['payload']['pull_request']['head']['user']['id']",
                           'payload__pull_request__head__user__node_id': "event_data['payload']['pull_request']['head']['user']['node_id']",
                           'payload__pull_request__head__user__type': "event_data['payload']['pull_request']['head']['user']['type']",
                           'payload__pull_request__head__repo__id': "event_data['payload']['pull_request']['head']['repo']['id']",
                           'payload__pull_request__head__repo__node_id': "event_data['payload']['pull_request']['head']['repo']['node_id']",
                           'payload__pull_request__head__repo__name': "event_data['payload']['pull_request']['head']['repo']['name']",
                           'payload__pull_request__head__repo__full_name': "event_data['payload']['pull_request']['head']['repo']['full_name']",
                           'payload__pull_request__head__repo__owner__login': "event_data['payload']['pull_request']['head']['repo']['owner']['login']",
                           'payload__pull_request__head__repo__owner__id': "event_data['payload']['pull_request']['head']['repo']['owner']['id']",
                           'payload__pull_request__head__repo__owner__node_id': "event_data['payload']['pull_request']['head']['repo']['owner']['node_id']",
                           'payload__pull_request__head__repo__owner__type': "event_data['payload']['pull_request']['head']['repo']['owner']['type']",
                           'payload__pull_request__head__repo__description': "event_data['payload']['pull_request']['head']['repo']['description']",
                           'payload__pull_request__head__repo__created_at': "event_data['payload']['pull_request']['head']['repo']['created_at']",
                           'payload__pull_request__head__repo__updated_at': "event_data['payload']['pull_request']['head']['repo']['updated_at']",
                           'payload__pull_request__head__repo__pushed_at': "event_data['payload']['pull_request']['head']['repo']['pushed_at']",
                           'payload__pull_request__head__repo__language': "event_data['payload']['pull_request']['head']['repo']['language']",
                           'payload__pull_request__head__repo__default_branch': "event_data['payload']['pull_request']['head']['repo']['default_branch']",
                           'payload__pull_request__base__label': "event_data['payload']['pull_request']['base']['label']",
                           'payload__pull_request__base__ref': "event_data['payload']['pull_request']['base']['ref']",
                           'payload__pull_request__base__sha': "event_data['payload']['pull_request']['base']['sha']",
                           'payload__pull_request__base__user__login': "event_data['payload']['pull_request']['base']['user']['login']",
                           'payload__pull_request__base__user__id': "event_data['payload']['pull_request']['base']['user']['id']",
                           'payload__pull_request__base__user__node_id': "event_data['payload']['pull_request']['base']['user']['node_id']",
                           'payload__pull_request__base__user__type': "event_data['payload']['pull_request']['base']['user']['type']",
                           'payload__pull_request__base__repo__id': "event_data['payload']['pull_request']['base']['repo']['id']",
                           'payload__pull_request__base__repo__node_id': "event_data['payload']['pull_request']['base']['repo']['node_id']",
                           'payload__pull_request__base__repo__name': "event_data['payload']['pull_request']['base']['repo']['name']",
                           'payload__pull_request__base__repo__full_name': "event_data['payload']['pull_request']['base']['repo']['full_name']",
                           'payload__pull_request__base__repo__owner__login': "event_data['payload']['pull_request']['base']['repo']['owner']['login']",
                           'payload__pull_request__base__repo__owner__id': "event_data['payload']['pull_request']['base']['repo']['owner']['id']",
                           'payload__pull_request__base__repo__owner__node_id': "event_data['payload']['pull_request']['base']['repo']['owner']['node_id']",
                           'payload__pull_request__base__repo__owner__type': "event_data['payload']['pull_request']['base']['repo']['owner']['type']",
                           'payload__pull_request__base__repo__description': "event_data['payload']['pull_request']['base']['repo']['description']",
                           'payload__pull_request__base__repo__created_at': "event_data['payload']['pull_request']['base']['repo']['created_at']",
                           'payload__pull_request__base__repo__updated_at': "event_data['payload']['pull_request']['base']['repo']['updated_at']",
                           'payload__pull_request__base__repo__pushed_at': "event_data['payload']['pull_request']['base']['repo']['pushed_at']",
                           'payload__pull_request__base__repo__language': "event_data['payload']['pull_request']['base']['repo']['language']",
                           'payload__pull_request__base__repo__default_branch': "event_data['payload']['pull_request']['base']['repo']['default_branch']",
                           'payload__pull_request__author_association': "event_data['payload']['pull_request']['author_association']",
                           'payload__pull_request__mergeable_state': "event_data['payload']['pull_request']['mergeable_state']",
                           'payload__pull_request__merged_by__login': "event_data['payload']['pull_request']['merged_by']['login']",
                           'payload__pull_request__merged_by__id': "event_data['payload']['pull_request']['merged_by']['id']",
                           'payload__pull_request__merged_by__node_id': "event_data['payload']['pull_request']['merged_by']['node_id']",
                           'payload__pull_request__merged_by__type': "event_data['payload']['pull_request']['merged_by']['type']"},
    'fork_event': {'id': "event_data['id']",
                   'type': "event_data['type']",
                   'public': "event_data['public']",
                   'created_at': "event_data['created_at']",
                   'actor__id': "event_data['actor']['id']", 'actor__login': "event_data['actor']['login']",
                   'actor__display_login': "event_data['actor']['display_login']",
                   'repo__id': "event_data['repo']['id']", 'repo__name': "event_data['repo']['name']",
                   'payload__forkee__id': "event_data['payload']['forkee']['id']",
                   'payload__forkee__node_id': "event_data['payload']['forkee']['node_id']",
                   'payload__forkee__name': "event_data['payload']['forkee']['name']",
                   'payload__forkee__full_name': "event_data['payload']['forkee']['full_name']",
                   'payload__forkee__private': "event_data['payload']['forkee']['private']",
                   'payload__forkee__owner__login': "event_data['payload']['forkee']['owner']['login']",
                   'payload__forkee__owner__id': "event_data['payload']['forkee']['owner']['id']",
                   'payload__forkee__owner__node_id': "event_data['payload']['forkee']['owner']['node_id']",
                   'payload__forkee__owner__type': "event_data['payload']['forkee']['owner']['type']",
                   'payload__forkee__owner__site_admin': "event_data['payload']['forkee']['owner']['site_admin']",
                   'payload__forkee__description': "event_data['payload']['forkee']['description']",
                   'payload__forkee__created_at': "event_data['payload']['forkee']['created_at']",
                   'payload__forkee__updated_at': "event_data['payload']['forkee']['updated_at']",
                   'payload__forkee__pushed_at': "event_data['payload']['forkee']['pushed_at']",
                   'payload__forkee__default_branch': "event_data['payload']['forkee']['default_branch']",
                   'payload__forkee__public': "event_data['payload']['forkee']['public']"},
    'pull_request_review_event': {'id': "event_data['id']", 'type': "event_data['type']",
                                  'public': "event_data['public']",
                                  'created_at': "event_data['created_at']", 'actor__id': "event_data['actor']['id']",
                                  'actor__login': "event_data['actor']['login']",
                                  'actor__display_login': "event_data['actor']['display_login']",
                                  'repo__id': "event_data['repo']['id']", 'repo__name': "event_data['repo']['name']",
                                  'payload__action': "event_data['payload']['action']",
                                  'payload__review__id': "event_data['payload']['review']['id']",
                                  'payload__review__node_id': "event_data['payload']['review']['node_id']",
                                  'payload__review__user__login': "event_data['payload']['review']['user']['login']",
                                  'payload__review__user__id': "event_data['payload']['review']['user']['id']",
                                  'payload__review__user__node_id': "event_data['payload']['review']['user']['node_id']",
                                  'payload__review__user__type': "event_data['payload']['review']['user']['type']",
                                  'payload__review__body': "event_data['payload']['review']['body']",
                                  'payload__review__commit_id': "event_data['payload']['review']['commit_id']",
                                  'payload__review__submitted_at': "event_data['payload']['review']['submitted_at']",
                                  'payload__review__state': "event_data['payload']['review']['state']",
                                  'payload__review__author_association': "event_data['payload']['review']['author_association']",
                                  'payload__pull_request__id': "event_data['payload']['pull_request']['id']",
                                  'payload__pull_request__node_id': "event_data['payload']['pull_request']['node_id']",
                                  'payload__pull_request__number': "event_data['payload']['pull_request']['number']",
                                  'payload__pull_request__state': "event_data['payload']['pull_request']['state']",
                                  'payload__pull_request__title': "event_data['payload']['pull_request']['title']",
                                  'payload__pull_request__user__login': "event_data['payload']['pull_request']['user']['login']",
                                  'payload__pull_request__user__id': "event_data['payload']['pull_request']['user']['id']",
                                  'payload__pull_request__user__node_id': "event_data['payload']['pull_request']['user']['node_id']",
                                  'payload__pull_request__user__type': "event_data['payload']['pull_request']['user']['type']",
                                  'payload__pull_request__body': "event_data['payload']['pull_request']['body']",
                                  'payload__pull_request__created_at': "event_data['payload']['pull_request']['created_at']",
                                  'payload__pull_request__updated_at': "event_data['payload']['pull_request']['updated_at']",
                                  'payload__pull_request__closed_at': "event_data['payload']['pull_request']['closed_at']",
                                  'payload__pull_request__merged_at': "event_data['payload']['pull_request']['merged_at']",
                                  'payload__pull_request__merge_commit_sha': "event_data['payload']['pull_request']['merge_commit_sha']",
                                  'payload__pull_request__head__label': "event_data['payload']['pull_request']['head']['label']",
                                  'payload__pull_request__head__ref': "event_data['payload']['pull_request']['head']['ref']",
                                  'payload__pull_request__head__sha': "event_data['payload']['pull_request']['head']['sha']",
                                  'payload__pull_request__head__user__login': "event_data['payload']['pull_request']['head']['user']['login']",
                                  'payload__pull_request__head__user__id': "event_data['payload']['pull_request']['head']['user']['id']",
                                  'payload__pull_request__head__user__node_id': "event_data['payload']['pull_request']['head']['user']['node_id']",
                                  'payload__pull_request__head__user__type': "event_data['payload']['pull_request']['head']['user']['type']",
                                  'payload__pull_request__head__repo__id': "event_data['payload']['pull_request']['head']['repo']['id']",
                                  'payload__pull_request__head__repo__node_id': "event_data['payload']['pull_request']['head']['repo']['node_id']",
                                  'payload__pull_request__head__repo__name': "event_data['payload']['pull_request']['head']['repo']['name']",
                                  'payload__pull_request__head__repo__full_name': "event_data['payload']['pull_request']['head']['repo']['full_name']",
                                  'payload__pull_request__head__repo__owner__login': "event_data['payload']['pull_request']['head']['repo']['owner']['login']",
                                  'payload__pull_request__head__repo__owner__id': "event_data['payload']['pull_request']['head']['repo']['owner']['id']",
                                  'payload__pull_request__head__repo__owner__node_id': "event_data['payload']['pull_request']['head']['repo']['owner']['node_id']",
                                  'payload__pull_request__head__repo__owner__type': "event_data['payload']['pull_request']['head']['repo']['owner']['type']",
                                  'payload__pull_request__head__repo__created_at': "event_data['payload']['pull_request']['head']['repo']['created_at']",
                                  'payload__pull_request__head__repo__updated_at': "event_data['payload']['pull_request']['head']['repo']['updated_at']",
                                  'payload__pull_request__head__repo__pushed_at': "event_data['payload']['pull_request']['head']['repo']['pushed_at']",
                                  'payload__pull_request__head__repo__size': "event_data['payload']['pull_request']['head']['repo']['size']",
                                  'payload__pull_request__head__repo__language': "event_data['payload']['pull_request']['head']['repo']['language']",
                                  'payload__pull_request__head__repo__has_issues': "event_data['payload']['pull_request']['head']['repo']['has_issues']",
                                  'payload__pull_request__head__repo__has_projects': "event_data['payload']['pull_request']['head']['repo']['has_projects']",
                                  'payload__pull_request__head__repo__has_downloads': "event_data['payload']['pull_request']['head']['repo']['has_downloads']",
                                  'payload__pull_request__head__repo__open_issues_count': "event_data['payload']['pull_request']['head']['repo']['open_issues_count']",
                                  'payload__pull_request__head__repo__open_issues': "event_data['payload']['pull_request']['head']['repo']['open_issues']",
                                  'payload__pull_request__head__repo__default_branch': "event_data['payload']['pull_request']['head']['repo']['default_branch']",
                                  'payload__pull_request__base__label': "event_data['payload']['pull_request']['base']['label']",
                                  'payload__pull_request__base__ref': "event_data['payload']['pull_request']['base']['ref']",
                                  'payload__pull_request__base__sha': "event_data['payload']['pull_request']['base']['sha']",
                                  'payload__pull_request__base__user__login': "event_data['payload']['pull_request']['base']['user']['login']",
                                  'payload__pull_request__base__user__id': "event_data['payload']['pull_request']['base']['user']['id']",
                                  'payload__pull_request__base__user__node_id': "event_data['payload']['pull_request']['base']['user']['node_id']",
                                  'payload__pull_request__base__user__type': "event_data['payload']['pull_request']['base']['user']['type']",
                                  'payload__pull_request__base__repo__id': "event_data['payload']['pull_request']['base']['repo']['id']",
                                  'payload__pull_request__base__repo__node_id': "event_data['payload']['pull_request']['base']['repo']['node_id']",
                                  'payload__pull_request__base__repo__name': "event_data['payload']['pull_request']['base']['repo']['name']",
                                  'payload__pull_request__base__repo__full_name': "event_data['payload']['pull_request']['base']['repo']['full_name']",
                                  'payload__pull_request__base__repo__owner__login': "event_data['payload']['pull_request']['base']['repo']['owner']['login']",
                                  'payload__pull_request__base__repo__owner__id': "event_data['payload']['pull_request']['base']['repo']['owner']['id']",
                                  'payload__pull_request__base__repo__owner__node_id': "event_data['payload']['pull_request']['base']['repo']['owner']['node_id']",
                                  'payload__pull_request__base__repo__owner__type': "event_data['payload']['pull_request']['base']['repo']['owner']['type']",
                                  'payload__pull_request__base__repo__created_at': "event_data['payload']['pull_request']['base']['repo']['created_at']",
                                  'payload__pull_request__base__repo__updated_at': "event_data['payload']['pull_request']['base']['repo']['updated_at']",
                                  'payload__pull_request__base__repo__pushed_at': "event_data['payload']['pull_request']['base']['repo']['pushed_at']",
                                  'payload__pull_request__base__repo__size': "event_data['payload']['pull_request']['base']['repo']['size']",
                                  'payload__pull_request__base__repo__language': "event_data['payload']['pull_request']['base']['repo']['language']",
                                  'payload__pull_request__base__repo__has_issues': "event_data['payload']['pull_request']['base']['repo']['has_issues']",
                                  'payload__pull_request__base__repo__has_projects': "event_data['payload']['pull_request']['base']['repo']['has_projects']",
                                  'payload__pull_request__base__repo__has_downloads': "event_data['payload']['pull_request']['base']['repo']['has_downloads']",
                                  'payload__pull_request__base__repo__open_issues_count': "event_data['payload']['pull_request']['base']['repo']['open_issues_count']",
                                  'payload__pull_request__base__repo__open_issues': "event_data['payload']['pull_request']['base']['repo']['open_issues']",
                                  'payload__pull_request__base__repo__default_branch': "event_data['payload']['pull_request']['base']['repo']['default_branch']",
                                  'payload__pull_request__author_association': "event_data['payload']['pull_request']['author_association']"},
    'push_event': {'id': "event_data['id']", 'type': "event_data['type']", 'public': "event_data['public']",
                   'created_at': "event_data['created_at']", 'actor__id': "event_data['actor']['id']",
                   'actor__login': "event_data['actor']['login']",
                   'actor__display_login': "event_data['actor']['display_login']",
                   'actor__gravatar_id': "event_data['actor']['gravatar_id']",
                   'actor__url': "event_data['actor']['url']", 'repo__id': "event_data['repo']['id']",
                   'repo__name': "event_data['repo']['name']", 'repo__url': "event_data['repo']['url']",
                   'payload__push_id': "event_data['payload']['push_id']",
                   'payload__size': "event_data['payload']['size']",
                   'payload__distinct_size': "event_data['payload']['distinct_size']",
                   'payload__ref': "event_data['payload']['ref']", 'payload__head': "event_data['payload']['head']",
                   'payload__before': "event_data['payload']['before']",
                   'payload__commits': "event_data['payload']['commits']"},
    'commit_comment_event': {
        "id": "event_data['id']",
        "type": "event_data['type']",
        "public": "event_data['public']",
        "created_at": "event_data['created_at']",
        "actor__id": "event_data['actor']['id']",
        "actor__login": "event_data['actor']['login']",
        "actor__display_login": "event_data['actor']['display_login']",
        "repo__id": "event_data['repo']['id']",
        "repo__name": "event_data['repo']['name']",
        "payload__comment__id": "event_data['payload']['comment']['id']",
        "payload__comment__node_id": "event_data['payload']['comment']['node_id']",
        "payload__comment__user__login": "event_data['payload']['comment']['user']['login']",
        "payload__comment__user__id": "event_data['payload']['comment']['user']['id']",
        "payload__comment__user__node_id": "event_data['payload']['comment']['user']['node_id']",
        "payload__comment__user__type": "event_data['payload']['comment']['user']['type']",
        "payload__comment__path": "event_data['payload']['comment']['path']",
        "payload__comment__commit_id": "event_data['payload']['comment']['commit_id']",
        "payload__comment__created_at": "event_data['payload']['comment']['created_at']",
        "payload__comment__updated_at": "event_data['payload']['comment']['updated_at']",
        "payload__comment__author_association": "event_data['payload']['comment']['author_association']",
        "payload__comment__body": "event_data['payload']['comment']['body']"
    }

    ,
    'issues_event': {'id': "event_data['id']", 'type': "event_data['type']", 'public': "event_data['public']",
                     'created_at': "event_data['created_at']", 'actor__id': "event_data['actor']['id']",
                     'actor__login': "event_data['actor']['login']",
                     'actor__display_login': "event_data['actor']['display_login']",
                     'repo__id': "event_data['repo']['id']", 'repo__name': "event_data['repo']['name']",
                     'payload__action': "event_data['payload']['action']",
                     'payload__issue__id': "event_data['payload']['issue']['id']",
                     'payload__issue__node_id': "event_data['payload']['issue']['node_id']",
                     'payload__issue__number': "event_data['payload']['issue']['number']",
                     'payload__issue__title': "event_data['payload']['issue']['title']",
                     'payload__issue__user__login': "event_data['payload']['issue']['user']['login']",
                     'payload__issue__user__id': "event_data['payload']['issue']['user']['id']",
                     'payload__issue__user__node_id': "event_data['payload']['issue']['user']['node_id']",
                     'payload__issue__user__type': "event_data['payload']['issue']['user']['type']",
                     'payload__issue__state': "event_data['payload']['issue']['state']",
                     'payload__issue__assignee__login': "event_data['payload']['issue']['assignee']['login']",
                     'payload__issue__assignee__id': "event_data['payload']['issue']['assignee']['id']",
                     'payload__issue__assignee__node_id': "event_data['payload']['issue']['assignee']['node_id']",
                     'payload__issue__assignee__type': "event_data['payload']['issue']['assignee']['type']",
                     'payload__issue__created_at': "event_data['payload']['issue']['created_at']",
                     'payload__issue__updated_at': "event_data['payload']['issue']['updated_at']",
                     'payload__issue__closed_at': "event_data['payload']['issue']['closed_at']",
                     'payload__issue__author_association': "event_data['payload']['issue']['author_association']",
                     'payload__issue__active_lock_reason': "event_data['payload']['issue']['active_lock_reason']",
                     'payload__issue__body': "event_data['payload']['issue']['body']",
                     'org__id': "event_data['org']['id']", 'org__login': "event_data['org']['login']"}
    ,
    'watch_event': {"id": "event_data['id']", "type": "event_data['type']", "public": "event_data['public']",
                    "created_at": "event_data['created_at']", "actor__id": "event_data['actor']['id']",
                    "actor__login": "event_data['actor']['login']",
                    "actor__display_login": "event_data['actor']['display_login']",
                    "actor__url": "event_data['actor']['url']",
                    "actor__avatar_url": "event_data['actor']['avatar_url']", "repo__id": "event_data['repo']['id']",
                    "repo__name": "event_data['repo']['name']", "repo__url": "event_data['repo']['url']",
                    "payload__action": "event_data['payload']['action']"}

}

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


def insert_into_ck(bulk_data, table_name,file_name=None):
    conn_info = get_ck_conn_info('ClickHouseLocal9000')
    ck_client = get_ck_client(conn_info)
    #
    sql_ = f"INSERT INTO TABLE {table_name} VALUES"
    try:
        count = ck_client.execute_with_params(sql_, bulk_data)
    except Exception as e:
        print(file_name,table_name)
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
                # if len(bulk_data) >= 50000:
                #     insert_into_ck(bulk_data, table_name)
                #     # 避免持续占用内存,而不释放
                #     bulk_data.clear()
                # return
            logger.info(f'文件{file_name} 总解析行数:{count}')
        pr_bulk_data = bulk_datas.get('pull_request_event')
        if pr_bulk_data:
            insert_into_ck(pr_bulk_data, 'cleaned_mini_pull_request_event',file_name)
            pr_bulk_data.clear()
        push_bulk_data = bulk_datas.get('push_event')
        if push_bulk_data:
            insert_into_ck(push_bulk_data, 'cleaned_mini_push_event_v3',file_name)
            push_bulk_data.clear()
    # 插入数据库
    # pr_bulk_data = bulk_datas.get('pull_request_event')
    # fork_bulk_data = bulk_datas.get('fork_event')
    # pr_review_bulk_data = bulk_datas.get('pull_request_review_event')
    # push_bulk_data = bulk_datas.get('push_event')
    # watch_bulk_data = bulk_datas.get('watch_event')
    # logger.info(watch_bulk_data)

    # if pr_bulk_data:
    #     insert_into_ck(pr_bulk_data, 'cleaned_mini_pull_request_event')
    # if fork_bulk_data:
    #     insert_into_ck(fork_bulk_data, 'fork_event_simple')
    # if pr_review_bulk_data:
    #     insert_into_ck(pr_review_bulk_data, 'cleaned_mini_pull_request_review_event')
    # if push_bulk_data:
    #     insert_into_ck(push_bulk_data, 'cleaned_mini_push_event_v2')
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

    with Pool(15) as pool:
        pool.map(all_event, json_names)

