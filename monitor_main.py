# -*- coding: utf-8 -*-
# @Time    : 2023/4/28 22:31
# @Author  : HuangSir
# @FileName: monitor_main.py
# @Software: PyCharm
# @Desc: 监控报表


import sys

sys.path.append('..')
import warnings

warnings.filterwarnings('ignore')

from log import log
import time
from apscheduler.schedulers.blocking import BlockingScheduler

from pushplus import daily_monitor_app,asy_monitor_app

from pytz import timezone
# 创建东八区时区对象
tz = timezone('Asia/Shanghai')

# 测试 -------------------------------------------------
daily_monitor_app()
# 开始任务 -------------------------------------------------
sched = BlockingScheduler(timezone=tz)
# 订单详情
@sched.scheduled_job('cron',hour=8)
def daily_monitor_job():
    '''订单详情'''
    log.logger.info(f"start daily_monitor_job at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")
    daily_monitor_app()
    log.logger.info(f"finish daily_monitor_job at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")

# 订单详情
@sched.scheduled_job('cron',minute=60)
def asy_monitor_job():
    '''订单详情'''
    log.logger.info(f"start asy_monitor_job at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")
    asy_monitor_app()
    log.logger.info(f"finish asy_monitor_job at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")

sched.start()