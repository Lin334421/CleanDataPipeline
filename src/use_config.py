from src.config_loader import ConfigManager, get_ck_conn_info

print(ConfigManager().get_config()['ClickHouseLocal9000'])
print(get_ck_conn_info('ClickHouseLocal9000'))