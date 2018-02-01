#!/usr/bin/env python3
# coding: utf-8
from logger import Log
from configparser import ConfigParser
import os
import functools
import queue
import datetime
import pandas as pd

__all__ = ['GLOBAL', 'MONGODB', 'logger', 'mqueue', 'get_stock_list', 'get_code_session',
           'get_end', 'tickRunning']

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
tickRunning = True


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

if __name__ == '__main__':
    name = GLOBAL('log_file').format(os.getpid())
    print(name)