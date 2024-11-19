import calendar

from src.config_loader import get_ck_client

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
for year in range(2015, 2024):
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
                    print(f"{year}-{month}-{i}-{j}")
                    need_get.append(f"{year}-{month}-{i}-{j}.json.gz")
print(len(need_get))
