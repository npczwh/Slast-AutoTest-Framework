#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import xml.etree.ElementTree as ET
from env_item_iterator import EnvItemIterator
from env_executor import EnvExecutor


class EnvAdapter(object):
    def __init__(self, name, config, path, log):
        self.name = name
        self.config = config
        self.path = path
        self.log = log
        self.msg = ''
        self.iterator = None
        self.conf = None
        self.hosts = None
        self.index = 0
        self.executors = []

    def parse_env(self, env, handler_name):
        for conf in env:
            attr = conf.attrib
            if not conf.text:
                d = {'hosts':self.hosts, 'attr':attr, 'value':None}
                d = {handler_name:d}
                self.iterator = EnvItemIterator([d], self.iterator)
                return True
            values = conf.text.split(';')
            if not len(values):
                self.msg = 'values in conf not found in %s' % self.config
                return False
            items = []
            for value in values:
                d = {'hosts': self.hosts, 'attr': attr, 'value': value}
                d = {handler_name:d}
                items.append(d)
            self.iterator = EnvItemIterator(items, self.iterator)
        return True

    def parse_root(self):
        handler_dict = {}
        tree = ET.parse(self.config)
        root = tree.getroot()
        self.hosts = root.attrib.get('hosts', None)
        # if not self.hosts:
        #     self.msg = 'hosts is not found in %s' % self.config
        #     return False
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
        self.log.debug('execute %s: %s' % (self.name, self.conf))
        for i in range(len(self.executors)):
            self.index = i + 1
            self.executors[i].parse_conf(self.conf)
            if not self.executors[i].execute():
                self.msg = self.executors[i].get_message()
                return False
        return True

    def clear(self):
        self.log.debug('clear %s: %s' % (self.name, self.conf))
        if not self.index:
            return True
        for i in range(self.index):
            if not self.executors[i].clear():
                self.msg = self.executors[i].get_message()
                return False
        self.index = 0
        return True

    def get_info(self):
        info = []
        for item in self.conf:
            info.append(item)
        return info

    def get_message(self):
        return self.msg
