#!/usr/bin/env python3
# coding: utf-8

import pymongo
import asyncio
import pandas as pd
import numpy as np
import json
import datetime
import click
import threading
import math
import os, sys

from tdx.engine import Engine, AsyncEngine, get_stock_type

from utils import get_code_session, get_stock_list, GLOBAL, logger, mqueue, tickRunning

from tickWriter import WriterTickMongo

class TEngine(AsyncEngine):
    def __init__(self, queue, *args, **kwargs):
        super(TEngine, self).__init__(*args, **kwargs)
        self.queue = queue

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

        trades_day_slice = 365
        times = int(math.ceil(len(trade_days) / trades_day_slice))
        for i in range(times):
            head = i * trades_day_slice
            tail = head + trades_day_slice
            if tail > len(trade_days):
                tail = len(trade_days)

            try:
                res = [self.get_transaction(code, int(trade_day)) for trade_day in
                   trade_days[head:tail]]
                completed, pending = self.aapi.run_until_complete(asyncio.wait(res))
            except ValueError as e:
                logger.error("code {0} from {1} to {2} error, {3}".format(
                    code, trade_days[head], trade_days[tail - 1], str(e)
                ))
                continue
            buf = []
            for t in completed:
                df = t.result()
                if not df.empty:
                    self.queue.put(df)

def getTickThread():
    print('start get tick thread')
    global tickRunning
    tickRunning = True
    # 0 - 3493
    if len(sys.argv) == 1:
        startCode = None
        endCode = None
    elif len(sys.argv) == 2:
        startCode = int(sys.argv[1])
        endCode = None
    else:
        startCode = int(sys.argv[1])
        endCode = int(sys.argv[2])
    aeg = TEngine(mqueue, ip='180.153.18.170', auto_retry=True, raise_exception=True)
    aeg.connect()
    stock_list = get_stock_list(aeg)
    # start = pd.Timestamp('20180126')
    start = pd.Timestamp(GLOBAL('start_date'))
    end = pd.Timestamp(GLOBAL('end_date'))
    # logger.info('pid: {0} starting..... from {1} to {2}, '
    #             'startCode:{3}, endCode:{4}'.format(os.getpid(), start, end, startCode, endCode))
    test_codes = ['300371']
    # for code in stock_list.code[startCode:endCode]:
    with click.progressbar(
            # stock_list.code[startCode:endCode],
            test_codes,
            label="Merging get tick datas:",
            item_show_func=lambda e: e if e is None else str(e[0]),
    ) as codes:
        for code in codes:
            logger.info('pid: {0} get code {1}'.format(os.getpid(), code))
            sessions = get_code_session(aeg, code, start, end)
            if len(sessions) == 0:
                continue
            trade_days = sessions.strftime("%Y%m%d").tolist()
            aeg.get_tick(code, trade_days)

    aeg.exit()
    tickRunning = False
    print("stop get tick thread")


def main():
    tickThread = threading.Thread(target=getTickThread)

    threads = []
    for x in range(5):
        wthread = WriterTickMongo(mqueue)
        threads.append(wthread)
    threads.append(tickThread)
    for thr in threads:
        thr.start()
    for thr in threads:
        if thr.isAlive():
            thr.join()

if __name__ == '__main__':
    main()