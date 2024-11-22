import calendar
from concurrent.futures import ThreadPoolExecutor

from src.config_loader import get_ck_client, ConfigManager
from src.data_download_v2 import download_gha_archive

"""follow_event"""
count = 0
sql_ = f"""
select search_key_gh_archive_year,search_key_gh_archive_month, search_key_gh_archive_day, search_key_gh_archive_hour
      from github_action_events
--       where
--           search_key_gh_archive_year in ('2018', '2019', '2020', '2021')
--           search_key_gh_archive_year in ('2022')
--         and search_key_gh_archive_month = '08'
      group by search_key_gh_archive_year, search_key_gh_archive_month, search_key_gh_archive_day,
               search_key_gh_archive_hour
"""
ck_client = get_ck_client('ClickHouseLocal9000')
results = ck_client.execute_no_params(sql_)
already_get = set()
for result in results:
    already_get.add(f"{result[0]}-{result[1]}-{result[2]}-{result[3]}")
need_get = []
# for year in range(2023, 2025):
for year in [2023]:

    for month in range(1, 13):
        day_count = calendar.monthrange(year, month)[1]
        if month < 10:
            month = '0' + str(month)
        for i in range(1, day_count + 1):
            if i < 10:
                i = '0' + str(i)
            for j in range(24):
                # print(f"{year}-{month}-{i}-{j}")
                if f"{year}-{month}-{i}-{j}" not in already_get:
                    # print(f"{year}-{month}-{i}-{j}")
                    # need_get.append(f"{year}-{month}-{i}-{j}.json.gz")
                    if year==2024 and month==11 and i>21 or year == 2024 and month==12:
                        continue
                    need_get.append(f"https://data.gharchive.org/{year}-{month}-{i}-{j}.json.gz")
print(len(need_get))

# with open('need_down_load_list.txt','w') as f:
#     f.write('\n'.join(need_get))
if __name__ == '__main__':
    proxy_url = "http://127.0.0.1:7890"
    root_path = ConfigManager().get_data_parents_dir()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for download_url in need_get:
            futures.append(executor.submit(download_gha_archive, download_url, root_path, proxy_url))
        for future in futures:
            future.result()