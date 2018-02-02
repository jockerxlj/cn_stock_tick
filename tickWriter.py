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
        self.stock_tick_db = self.conn.stock_tick
        if GLOBAL('life') == 'production':
            self.post = self.stock_tick_db.tick
        else:
            self.post = self.stock_tick_db.tick_test

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
                    self.post.insert(buf)
                    buf.clear()
            if len(buf):
                self.post.insert(buf)