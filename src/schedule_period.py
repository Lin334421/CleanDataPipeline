import schedule
import time

from src.update import task_func


def my_task():
    print("任务执行中...")
    print(time.time())


# 每 60 秒执行一次
schedule.every(1800).seconds.do(task_func)

# 开始循环
while True:
    schedule.run_pending()  # 执行所有到期任务
    time.sleep(2)

# task_func()