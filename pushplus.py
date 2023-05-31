# -*- coding: utf-8 -*-
# @Time    : 2022/11/16 14:20
# @Author  : HuangSir
# @FileName: pushplus.py
# @Software: PyCharm
# @Desc: 消息推送

import sys

sys.path.append('..')
import warnings

warnings.filterwarnings('ignore')

import requests
import pandas as pd
from db_url import dbUrls,risk_url,query
from sql import sql,asy_sql

pd.set_option('display.width', 1000)
pd.set_option('colheader_justify', 'center')
from log import log
import time


def send_wechat(msg, title: str, template='html'):
    '''消息推送'''
    token = 'c58f07cddf1640c2ae13eac8e2cbee38'
    topic = 'eg002'
    # html 格式化
    if template == 'html' and isinstance(msg, pd.DataFrame):
        msg = msg.to_html(classes='mystyle', index=False)
        msg = f"""
            <html>
              <head><title>{title}</title></head>
              <link rel="stylesheet" type="text/css" href="pushplus.css"/>
              <body>
                {msg}
              </body>
            </html>.
            """
    url = f"https://www.pushplus.plus/send?token={token}&title={title}&content={msg}&topic={topic}&template={template}"

    r = requests.get(url=url, verify=False)
    log.logger.info(f"send {r.text} at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")


def daily_monitor_app():
    '''日常监控,每天定时发送
    模型监控
    '''
    for app,url in dbUrls.items():
        time.sleep(60)
        log.logger.info(f"开始执行:{app},{url}")
        dt = query(url=url,sql=sql)
        log.logger.info(f"查询结果:{dt}")
        # 过滤
        dt = dt[dt.到期订单>0]
        if dt.empty:
            continue

        dt['到期订单'] = dt['到期订单'].fillna(0).astype(int)
        dt['放款率'] = dt['放款率'].apply(lambda  x:str(round(x,3)*100)+'%')
        dt = dt.fillna('null')
        send_wechat(msg=dt,title=f"{app}")
        log.logger.info(f"完成报送,{app},{url}")

def asy_monitor_app():
    '''
    异步程序监控,存在一个小时前订单未处理报警
    :return:
    '''
    log.logger.info(f"开始执行异步程序监控")
    dt = query(risk_url,asy_sql)
    log.logger.info(f"查询结果:{dt}")
    if not dt.empty:
        send_wechat(msg=dt,title="异步程序异常")
        log.logger.info("完成异步程序异常报送")
