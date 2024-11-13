from fabric import Connection

def run_code_on_remote(host, user, password, code):
    conn = Connection(host=host, user=user, connect_kwargs={"password": password})
    result = conn.run(f'python3 -c "{code}"', hide=True)
    # 标准输出和错误输出
    return result.stdout, result.stderr

# 定义远程机器
machines = [

]

# 要运行的代码
code = """
print('Hello from Fabric!')
"""

# 在多台机器上执行代码
for machine in machines:
    output, error = run_code_on_remote(machine["host"], machine["user"], machine["password"], code)
    print(f"Output from {machine['host']}:\n{output}")
    if error:
        print(f"Error from {machine['host']}:\n{error}")
