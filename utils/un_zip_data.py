import gzip
import calendar
import time
from loguru import logger
logger.add('un_zip.log')
year_list = [2024]
month_list = [5]
data_parent_path = '/opt/mission_area/gha_data/'
def un_gzip(gz_filename):
    try:
        filename = gz_filename.replace('.gz', '')
        g_file = gzip.GzipFile(f'{data_parent_path}{gz_filename}')
        open(f'{data_parent_path}{filename}', 'wb+').write(g_file.read())
        g_file.close()
        logger.info(f"successful unzip {gz_filename}")
    except Exception as e:
        logger.error(e)
        logger.error(f"unzip {gz_filename} failed")


for year in year_list:
    count = 0
    for month in month_list:
        day_count = calendar.monthrange(year, month)[1]
        if month < 10:
            month = '0' + str(month)
        for i in range(1, day_count + 1):
            if i < 10:
                i = '0' + str(i)
            for j in range(24):
                un_gzip(f"{year}-{month}-{i}-{j}.json.gz")
                count+=1

