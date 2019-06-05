#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import xml.etree.ElementTree as ET
from env_item_iterator import EnvItemIterator
from handler_executor import HandlerExecutor
from env_executor import EnvExecutor
from func import *


class EnvAdapter(object):
    def __init__(self, name, config, path, log):
        self.name = name
        self.config = config
        self.path = path
        self.log = log
        self.msg = ''
        self.iterator = None
        self.conf = None
        self.executors = []

    def parse_env(self, env, handler_name):
        for conf in env:
            attr = conf.attrib
            if not conf.text:
                d = {handler_name:[attr, None]}
                self.iterator = EnvItemIterator([d], self.iterator)
                return True
            params = conf.text.split(';')
            items = []
            for param in params:
                d = {handler_name:[attr, param]}
                items.append(d)
            self.iterator = EnvItemIterator(items, self.iterator)
        return True

    def parse_root(self):
        handler_dict = {}
        tree = ET.parse(self.config)
        root = tree.getroot()
        for env in root:
            attr = env.attrib
            handler_name = attr.get('handler', None)
            if handler_name:
                if not handler_dict.get(handler_name, None):
                    executor = EnvExecutor(handler_name, self.path, self.log)
                    self.executors.append(executor)
                    handler_dict[handler_name] = True
            else:
                self.msg = 'handler name is not found in %s' % self.config
                return False
            if not self.parse_env(env, handler_name):
                return False
        return True

    def init(self):
        if not self.parse_root():
            return False
        self.iterator = self.iterator.get_root()
        for executor in self.executors:
            if not executor.init():
                self.msg = executor.get_message()
                return False
        return True

    def to_next(self):
        if self.iterator.has_next():
            self.conf = self.iterator.next([])
            return True
        else:
            return False

    def is_last(self):
        if self.iterator.has_next():
            return False
        else:
            return True

    def reset(self):
        self.iterator.reset()

    def execute(self):
        print 'exec %s: %s' % (self.name, self.conf)
        for executor in self.executors:
            executor.parse_conf(self.conf)
            if not executor.execute():
                self.msg = executor.get_message()
                return False
        return True

    def clear(self):
        print 'clear %s: %s' % (self.name, self.conf)
        for executor in self.executors:
            if not executor.clear():
                self.msg = executor.get_message()
                return False
        return True

    def get_info(self):
        return self.conf

    def get_message(self):
        return self.msg
