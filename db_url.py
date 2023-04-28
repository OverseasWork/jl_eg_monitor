# -*- coding: utf-8 -*-
# @Time    : 2022/10/23 23:27
# @Author  : HuangSir
# @FileName: db_url.py
# @Software: PyCharm
# @Desc:

"""
商户数据库 ######################################################################
"""

from urllib.parse import quote_plus as urlquote

eg02 = f"""mysql+mysqlconnector://eg02_cashhome:{urlquote('eg02_cashhome_!@#$Q5EnUS')}@rm-gw86q930f974op7kueo.mysql.germany.rds.aliyuncs.com/cash_eg02?charset=utf8"""

eg03 = f"""mysql+mysqlconnector://eg03_cashshawarma:{urlquote('eg03_cashshawarma_!@#$Q5EnUS')}@rm-gw86q930f974op7kueo.mysql.germany.rds.aliyuncs.com/cash_eg03?charset=utf8"""

eg04 = f"""mysql+mysqlconnector://eg04_supercash:{urlquote('eg04_supercash_!@#$Q5EnUS')}@rm-gw8id623b2e4b62ga2o.mysql.germany.rds.aliyuncs.com/cash_eg04?charset=utf8"""

eg06 = f"""mysql+mysqlconnector://eg06_eternal:{urlquote('eg06_eternal_!@#$Q5EnUS')}@rm-gw8id623b2e4b62ga2o.mysql.germany.rds.aliyuncs.com/cash_eg06?charset=utf8"""

eg07 = f"""mysql+mysqlconnector://eg07_pyramidcash:{urlquote('eg07_pyramidcash_!@#$Q5EnUS')}@rm-gw87d6x8wr33227bhno.mysql.germany.rds.aliyuncs.com/cash_eg07?charset=utf8"""


dbUrls = {
    "cashhome":eg02,
    "cashshawarma":eg03,
    "supercash":eg04,
    "eternal":eg06,
    "pyramidcash":eg07
}


from sqlalchemy import create_engine
import pandas as pd

def query(url:str,sql: str):
    '''数据查询'''
    eng = create_engine(url)
    with eng.connect() as conn:
        dt = pd.read_sql_query(sql=sql, con=conn)
    return dt