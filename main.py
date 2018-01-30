#!/usr/bin/env python3
# coding: utf-8

import pymongo
import asyncio
import pandas as pd
import numpy as np
import json
import datetime

from tdx.engine import Engine, AsyncEngine, get_stock_type


def get_stock_list(engine):
    with engine.connect():
         return engine.stock_list

def get_code_session(engine, code, start, end):
    with engine.connect():
        daily_bar = engine.get_security_bars(code, '1d', start, end)
        if daily_bar is None or daily_bar.empty:
            return []
        session = daily_bar.index
        return session
    raise Exception("engine connect failed!!!")



class TEngine(AsyncEngine):
    def __init__(self, *args, **kwargs):
        super(TEngine, self).__init__(*args, **kwargs)

    async def get_transaction(self, code, date):
        res = []
        start = 0
        while True:
            data = await self.aapi.get_history_transaction_data(get_stock_type(code), code, start, 2000,
                                                                date)
            if not data:
                break
            start += 2000
            res = data + res

        if len(res) == 0:
            return pd.DataFrame()
        df = self.api.to_df(res).assign(date=date)
        df.index = pd.to_datetime(str(date) + " " + df["time"])
        df['code'] = code
        return df.drop("time", axis=1)

    # 股票所有交易日的分笔数据
    def get_tick(self, code, trade_days):
        conn = pymongo.MongoClient('192.168.0.114', 27016)
        stock_tick_db = conn.stock_tick
        post = stock_tick_db.tick
        # post.insert({'a':123423143241234, 'b': '43545666'})

        res = [self.get_transaction(code, trade_day) for trade_day in
               trade_days]
        completed, pending = self.aapi.run_until_complete(asyncio.wait(res))
        buf = []
        for t in completed:
            df = t.result()
            if not df.empty:
                map = dict()
                map['date'] = str(df.date[0])
                map['code'] = df.code[0]
                map['time'] = df.index.tolist()
                map['buyorsell'] = df.buyorsell.tolist()
                map['buyorsell'] = [int(x) for x in map['buyorsell']]
                map['price'] = df.price.tolist()
                map['vol'] = df.vol.tolist()
                map['vol'] = [int(x) for x in map['vol']]
                print(map)
                buf.append(map)
                if len(buf) == 1000:
                    # post.insert(buf)
                    buf.clear()
        # post.insert(buf)




if __name__ == '__main__':
    eg = Engine(auto_retry=True, multithread=True, best_ip=True, thread_num=1, raise_exception=True)
    stock_list = get_stock_list(eg)
    aeg = TEngine(ip='202.108.253.130', raise_exception=True)
    start = pd.Timestamp('20180126')
    now = datetime.datetime.now()
    end = datetime.date.today()
    if now.time() < datetime.time(15, 5):
        end = end - datetime.timedelta(days=1)
    end = pd.Timestamp(end)
    print('starting..... from {0} to {1}.'.format(start, end))
    with aeg.connect():
        size = len(stock_list.code)
        for code in stock_list.code[:int(size*0.3)]:
            sessions = get_code_session(eg, code, start, end)
            if len(sessions) == 0:
                continue
            trade_days = map(int, sessions.strftime("%Y%m%d"))
            aeg.get_tick(code, trade_days)