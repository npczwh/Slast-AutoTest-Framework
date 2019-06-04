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
        self.param = None
        self.executors = []

    def create_iterator(self, conf, handler_name, type):
        if not len(conf):
            self.iterator = EnvItemIterator(handler_name, type, [None], self.iterator)
            return True
        items = []
        for param in conf:
            attr = param.attrib
            name = attr.get('name', None)
            if not name:
                self.msg = 'param name is not found in %s(%s)' % (self.config, handler_name)
                return False
            l = param.text.split(';')
            for p in l:
                items.append([name, p])
            self.iterator = EnvItemIterator(handler_name, type, items, self.iterator)
        return True

    def parse_env(self, env, handler_name):
        for conf in env:
            attr = conf.attrib
            type = attr.get('type', None)
            if type:
                if not self.create_iterator(conf, handler_name, type):
                    return False
            else:
                self.msg = 'type name is not found in %s(%s)' % (self.config, handler_name)
                return False
        return True

    def parse_root(self):
        tree = ET.parse(self.config)
        root = tree.getroot()
        for env in root:
            attr = env.attrib
            handler_name = attr.get('handler', None)
            if handler_name:
                executor = HandlerExecutor(handler_name, self.path, self.log)
                self.executors.append(executor)
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
        return True

    def to_next(self):
        if self.iterator.has_next():
            self.param = self.iterator.next([])
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
        print 'exec %s: %s' % (self.name, self.param)
        return True

    def clear(self):
        print 'clear %s: %s' % (self.name, self.param)
        return True

    def get_info(self):
        return str(self.param) + '\n'

    def get_message(self):
        return self.msg
