#!/usr/bin/env python3
# coding: utf-8
from logger import Log
from configparser import ConfigParser
import os
import functools
import queue
import datetime
import pandas as pd
import json

__all__ = ['GLOBAL', 'MONGODB', 'logger', 'mqueue', 'get_stock_list', 'get_code_session',
           'get_end', 'globalvar']

# global var
_cfg = ConfigParser()
_cfg.read('config/tick.conf')
GLOBAL = functools.partial(_cfg.get, 'global')
MONGODB = functools.partial(_cfg.get, 'mongodb')

if GLOBAL('life') == 'development':
    logger = Log('log/log.txt')
elif GLOBAL('life') == 'production':
    logger = Log(GLOBAL('log_file').format(os.getpid()))

mqueue = queue.Queue()


def get_stock_list(engine):
    return engine.stock_list


def get_code_session(engine, code, start, end):
    daily_bar = engine.get_security_bars(code, '1d', start, end)
    if daily_bar is None or daily_bar.empty:
        return []
    session = daily_bar.index
    return session


def get_end():
    '''
    根据now是否收盘返回end day，收盘返回今天，未收盘返回昨天
    '''
    now = datetime.datetime.now()
    end = datetime.date.today()
    if now.time() < datetime.time(15, 5):
        end = end - datetime.timedelta(days=1)
    end = pd.Timestamp(end)
    return end

class GlobalMap:
    # 拼装成字典构造全局变量  借鉴map  包含变量的增删改查
    map = {}

    def set_map(self, key, value):
        if(isinstance(value, dict)):
            value = json.dumps(value)
        self.map[key] = value

    def set(self, **kv):
        try:
            for key_, value_ in kv.items():
                self.map[key_] = value_
        except BaseException as msg:
            print(msg)
            raise msg

    def del_map(self, key):
        try:
            del self.map[key]
            return self.map
        except KeyError:
            print("key:'" + str(key) + "'  不存在")

    def get(self, *args):
        try:
            dic = {}
            for key in args:
                if len(args) == 1:
                    dic = self.map[key]
                elif len(args) == 1 and args[0] == 'all':
                    dic = self.map
                else:
                    dic[key]=self.map[key]
            return dic
        except KeyError:
            print("key:'" + str(key) + "'  不存在")
            return 'Null_'


globalvar = GlobalMap()
globalvar.set(tickRunning=True)

if __name__ == '__main__':
    name = GLOBAL('log_file').format(os.getpid())
    print(name)