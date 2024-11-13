import json

from src.config_loader import ConfigManager


def default_type(value):
    if isinstance(value, str):
        return ''
    elif isinstance(value, int):
        return 0
    elif isinstance(value, bool):
        return 0
    elif isinstance(value, list):
        if isinstance(value[0], str):
            return [""]
    return value


def flatten_json(nested_json, parent_key='', sep='__'):
    items = []
    replace_obj = "']['"
    for key, value in nested_json.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):  # 如果值是字典，递归展开
            items.extend(flatten_json(value, new_key, sep=sep).items())
        else:
            items.append((new_key, [f"event_data['{new_key.replace('__', replace_obj)}']", default_type(value)]))

    return dict(items)


# 示例输入

# 扁平化 JSON 数据
gha_final_default_type = {
    "search_key_updated_at": 0,
    "search_key_event_type": "",
    "search_key_gh_archive_year": "",
    "search_key_gh_archive_month": "",
    "search_key_gh_archive_hour": "",
    "search_key_gh_archive_day": "",
    "search_key_owner": "",
    "search_key_repo": "",
    "search_key_id": ""
}
res_map = {}
tplt_map = {}
with open('/Users/jonas/PycharmProjects/CleanDataPipeline/download_data/create_table_tplt.json', 'r') as f:
    create_table_tplt = json.load(f)
    create_table_tplt['parse_data'] = {
        "search_key_updated_at": 0,
        "search_key_event_type": "",
        "search_key_gh_archive_year": "",
        "search_key_gh_archive_month": "",
        "search_key_gh_archive_hour": "",
        "search_key_gh_archive_day": "",
        "search_key_owner": "",
        "search_key_repo": "",
        "search_key_id": ""
    }
for file_name in (
        'create_event.json',
        'delete_event.json',
        'fork_event.json',
        'gollum_event.json',
        'issue_comment_event.json',
        'issues_event.json',
        'member_event.json',
        'public_event.json',
        'pull_request_event.json',
        'pull_request_review_event.json',
        'pull_request_review_comment_event.json',
        'push_event.json',
        'release_event.json',
        'watch_event.json',
        'commit_comment_event.json'
):

    with open(f'{ConfigManager().get_data_parents_dir()}/{file_name}', 'r') as f:
        nested_json = json.load(f)
    flatten_json_data = flatten_json(nested_json)
    res_map[file_name[:-5]] = flatten_json_data
    # 打印结果
    for key, value in flatten_json_data.items():
        # print(f"{key}: {value}")
        if key.endswith('_at'):
            gha_final_default_type[key] = "1970-01-01T00:00:00Z"
            create_table_tplt['parse_data'][key] = "1970-01-01T00:00:00Z"
        else:
            if key == 'payload__pages':
                gha_final_default_type["payload__pages.page_name"] = [""]
                gha_final_default_type["payload__pages.title"] = [""]
                gha_final_default_type["payload__pages.summary"] = [""]
                gha_final_default_type["payload__pages.action"] = [""]
                gha_final_default_type["payload__pages.sha"] = [""]

            elif key == 'payload__issue__assignees':
                gha_final_default_type["payload__issue__assignees.login"] = [""]
                gha_final_default_type["payload__issue__assignees.id"] = [0]
                gha_final_default_type["payload__issue__assignees.node_id"] = [""]
                gha_final_default_type["payload__issue__assignees.type"] = [""]
            elif key == 'payload__pull_request__assignees':
                gha_final_default_type["payload__pull_request__assignees.login"] = [""]
                gha_final_default_type["payload__pull_request__assignees.id"] = [0]
                gha_final_default_type["payload__pull_request__assignees.node_id"] = [""]
                gha_final_default_type["payload__pull_request__assignees.type"] = [""]
            elif key == 'payload__pull_request__requested_reviewers':
                gha_final_default_type["payload__pull_request__requested_reviewers.login"] = [""]
                gha_final_default_type["payload__pull_request__requested_reviewers.id"] = [0]
                gha_final_default_type["payload__pull_request__requested_reviewers.node_id"] = [""]
                gha_final_default_type["payload__pull_request__requested_reviewers.type"] = [""]
            elif key == 'payload__commits':
                gha_final_default_type["payload__commits.sha"] = [""]
                gha_final_default_type["payload__commits.author__email"] = [""]
                gha_final_default_type["payload__commits.author__name"] = [""]
                gha_final_default_type["payload__commits.message"] = [""]
            else:
                gha_final_default_type[key] = value[1]
            create_table_tplt['parse_data'][key] = value[1]

    print(file_name, flatten_json_data)
    temp_flatten_json_data = {}
    for k, v in flatten_json_data.items():
        temp_flatten_json_data[k] = v[0]
    tplt_map[file_name[:-5]] = temp_flatten_json_data
    print("---" * 100)
# tplt_map 是用来取值的
with open('gha_tplt.json', 'w') as f:
    json.dump(tplt_map, f, indent=4)

print(gha_final_default_type)
# res 取默认值的
with open('gha_final_default_type.json', 'w') as f:
    json.dump(gha_final_default_type, f, indent=4)

# create_table_tplt 是用来建表的

with open(f'{ConfigManager().get_data_parents_dir()}/create_table_tplt.json', 'w') as f:
    json.dump(create_table_tplt, f, indent=4)

from download_data import ck_create_table
