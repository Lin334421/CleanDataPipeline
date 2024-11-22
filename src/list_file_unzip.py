from src.config_loader import ConfigManager
from src.listfile import list_files
from utils.un_zip_data import un_gzip_v2

root_path = ConfigManager().get_data_parents_dir()

gz_file,_ = list_files(root_path)
print(gz_file)
for file in gz_file:
    un_gzip_v2(file)