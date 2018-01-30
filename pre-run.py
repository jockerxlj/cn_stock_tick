#!/usr/bin/env python3
# coding: utf-8

import pymongo
from tdx.engine import Engine

def get_stock_list(engine):
    with engine.connect():
         return engine.stock_list

if __name__ == '__main__':
    conn = pymongo.MongoClient('192.168.0.114', 27016)
    stock_tick_db = conn.stock_tick
    code_finishing_post = stock_tick_db.code_finishing
    eg = Engine(auto_retry=True, multithread=True, best_ip=True, thread_num=1, raise_exception=True)
    stock_list = get_stock_list(eg)
    index = 1
    for code in stock_list.code:
        code_finishing_post.insert({str(index): code})
        index = index + 1