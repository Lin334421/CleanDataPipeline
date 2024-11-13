import json
import re
import numpy
import pandas as pd

regex = r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([' \
        r'0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$'

match_iso8601 = re.compile(regex).match


# 判断是不是iso8601 格式字符串
def validate_iso8601(str_val):
    try:
        if match_iso8601(str_val) is not None:
            return True
    except:
        pass
    return False


def np_type_2_py_type(row, template='None'):
    if isinstance(row, numpy.int64):
        row = int(row)
    elif isinstance(row, dict):
        row = str(row).replace(": ", ":")

    elif isinstance(row, numpy.bool_):
        row = int(bool(row))
    elif row is None:
        # ATTN : Sometime a 'int' variable could be None.
        if isinstance(template, int):
            row = 0
        elif isinstance(template, str):
            row = "null"
        else:
            row = "null"
    elif isinstance(row, bool):
        row = int(row)
    elif isinstance(row, numpy.float64):
        row = float(row)
    return row


def py2ck_type(data_type):
    type_init = "String"
    if isinstance(data_type, str):
        if validate_iso8601(data_type):
            type_init = "DateTime64(3)"
    elif isinstance(data_type, int):
        type_init = "Int64"
    elif isinstance(data_type, float):
        type_init = "Float64"
    elif isinstance(data_type, list):
        if isinstance(data_type[0], str):
            if validate_iso8601(data_type[0]):
                type_init = "Array(DateTime64(3))"
            else:
                type_init = "Array(String)"
        elif isinstance(data_type[0], int):
            type_init = "Array(Int64)"
        elif isinstance(data_type, float):
            type_init = "Array(Float64)"
    return type_init


def create_ck_table(df,
                    distributed_key="rand()",
                    database_name="default",
                    table_name="default_table",
                    cluster_name="",
                    table_engine="MergeTree",
                    order_by= [],
                    partition_by="",
                    clickhouse_server_info=None):
    if not distributed_key:
        distributed_key = "rand()"
    if not database_name:
        database_name = "default"
    # 存储最终的字段
    ck_data_type = []
    # 确定每个字段的类型 然后建表
    for index, row in df.iloc[0].items():
        # 去除包含raw_data的前缀
        if index.startswith('raw_data'):
            index = index[9:]
        index = index.replace('.', '__')
        # 设定一个初始的类型
        data_type_outer = f"`{index}` String"
        # 将数据进行类型的转换，有些类型但是pandas中独有的类型
        row_py_type = np_type_2_py_type(row)
        # 如果row的类型是列表
        if isinstance(row_py_type, list):
            # 解析列表中的内容
            # 如果是字典就将 index声明为nested类型的
            # 拿出数组中的一个，方式需要保证包含数据，如果数据不全就会出问题
            if row_py_type:
                if isinstance(row_py_type[0], dict):
                    # type_list存储所有数组中套字典中字典的类型
                    type_list = []
                    for key in row_py_type[0]:
                        # 再进行类型转换一次，可能有bool类型和Nonetype
                        if isinstance(row_py_type[0].get(key), dict):
                            df_inner = pd.json_normalize({key:row_py_type[0].get(key)})
                            for inner_key, row_ in df_inner.iloc[0].items():
                                # print(index.replace('.','__'),row)
                                one_of_field = np_type_2_py_type(row_)
                                ck_type = py2ck_type(data_type=one_of_field)
                                data_type = f"{inner_key.replace('.','__')} {ck_type}"
                                type_list.append(data_type)
                            continue

                        one_of_field = np_type_2_py_type(row_py_type[0].get(key))
                        # 映射ck的类型

                        ck_type = py2ck_type(data_type=one_of_field)
                        # 拼接字段和类型
                        data_type = f"{key} {ck_type}"
                        type_list.append(data_type)

                    one_nested_type = ",".join(type_list)
                    data_type_outer = f"`{index}` Nested({one_nested_type})"
                else:
                    # 声明为数组类型
                    one_of_field = np_type_2_py_type(row_py_type[0])
                    ck_type = py2ck_type(one_of_field)
                    data_type_outer = f"`{index}` Array({ck_type})"
        else:
            data_type_outer = f"`{index}` {py2ck_type(row_py_type)}"

        ck_data_type.append(data_type_outer)
    result = ",\r\n".join(ck_data_type)
    create_local_table_ddl = f'''
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name}_local
    on cluster {cluster_name}({result}) Engine={table_engine}
    '''
    create_distributed_ddl = f'''
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
    on cluster {cluster_name} as {database_name}.{table_name}_local
    Engine= Distributed({cluster_name}, {database_name},{table_name}_local,{distributed_key})
    '''
    if partition_by:
        partition_by = tuple(partition_by)
        create_local_table_ddl = f'{create_local_table_ddl} PARTITION BY {partition_by}'
    if order_by:
        order_by_str = ""
        for i in range(len(order_by)):
            if i != len(order_by) - 1:
                order_by_str = f'{order_by_str}{order_by[i]},'
            else:
                order_by_str = f'{order_by_str}{order_by[i]}'
        create_local_table_ddl = f'{create_local_table_ddl} ORDER BY ({order_by_str})'
    with open('create_table_tplt.sql', 'w') as f:
        f.write(create_local_table_ddl)
        f.write(create_distributed_ddl)

    print(create_local_table_ddl)

    print(create_distributed_ddl)


with open('/Users/jonas/PycharmProjects/CleanDataPipeline/download_data/create_table_tplt.json', 'r') as f:
    params = json.load(f)

table_name = params["table_name"]
cluster_name = params["cluster_name"]
table_engine = params["table_engine"]
partition_by = params["partition_by"]
order_by = params.get("order_by")
parse_data = params.get("parse_data")
database_name = params.get("database_name")
distributed_key = params.get("distributed_key")
df = pd.json_normalize(parse_data)
create_ck_table(df=df,
                database_name=database_name,
                distributed_key=distributed_key,
                cluster_name=cluster_name,
                table_name=table_name,
                table_engine=table_engine,
                partition_by=partition_by,
                order_by=order_by)
