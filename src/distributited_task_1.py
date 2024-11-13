import paramiko


def execute_remote_code(host, username, password, code):
    # 创建 SSH 客户端
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # 连接远程主机
    client.connect(hostname=host, username=username, password=password)

    # 执行远程代码
    stdin, stdout, stderr = client.exec_command(f'python3 -c {code}')

    # 获取执行结果
    output = stdout.read().decode()
    error = stderr.read().decode()

    # 关闭连接
    client.close()

    return output, error


# 远程机器信息
machines = [

]

# 要运行的代码
code = """
print("Hello from remote machine!")
"""

# 在两台机器上执行代码
for machine in machines:
    output, error = execute_remote_code(machine["host"], machine["username"], machine["password"], code)
    print(f"Output from {machine['host']}:\n{output}")
    if error:
        print(f"Error from {machine['host']}:\n{error}")
