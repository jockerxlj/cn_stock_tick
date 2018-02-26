#!/usr/bin/env python3
# coding: utf-8


import queue
import threading
import pymongo

from utils import GLOBAL, MONGODB, logger, globalvar


class WriterTickMongo(threading.Thread):
    def __init__(self, queue):
        super(WriterTickMongo, self).__init__()
        self.queue = queue
        self.conn = pymongo.MongoClient(MONGODB('db_host'), int(MONGODB('db_port')))

        if GLOBAL('life') == 'production':
            self.stock_tick_db = self.conn.stock_tick
        else:
            # self.post = self.stock_tick_db.tick_test
            self.stock_tick_db = self.conn.stock_tick_test

    def run(self):
        print('start mongo write tick thread')
        while self.queue.empty() is False or globalvar.get('tickRunning') is True:
            df = self.queue.get()
            buf = []
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
                # logger.debug(map)
                buf.append(map)
                if len(buf) == 1000:
                    for map in buf:
                        self.create_post_if_not_exist(map['code'])
                        self.stock_tick_db[str(map['code'])].insert(map)
                    buf.clear()
            if len(buf):
                for map in buf:
                    self.create_post_if_not_exist(map['code'])
                    self.stock_tick_db[str(map['code'])].insert(map)

    def create_post_if_not_exist(self, code):
        if str(code) not in self.stock_tick_db.collection_names():
            self.stock_tick_db[str(code)].create_index(
                [("code", pymongo.DESCENDING), ("date", pymongo.DESCENDING)], unique=True)