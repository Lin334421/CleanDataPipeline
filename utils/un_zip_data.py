import gzip
from loguru import logger

from src.config_loader import ConfigManager


# logger.add('un_zip.log')

def un_gzip_v2(gz_filename):
    data_parents_dir = ConfigManager().get_data_parents_dir()
    year = gz_filename.split('-')[0]
    parents_dir = data_parents_dir
    is_successful_unzip = False
    try:
        filename = gz_filename.replace('.gz', '')
        g_file = gzip.GzipFile(f'{parents_dir}/{gz_filename}')
        open(f'{parents_dir}/{filename}', 'wb+').write(g_file.read())
        g_file.close()
        logger.info(f"successful unzip {gz_filename}")
        is_successful_unzip =True
    except Exception as e:
        logger.error(e)
        logger.error(f"unzip {gz_filename} failed")
    return is_successful_unzip


