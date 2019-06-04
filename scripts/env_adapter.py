#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from env_item_iterator import EnvItemIterator
from env_executor import EnvExecutor


class EnvAdapter(object):
    def __init__(self, name, config, log):
        self.name = name
        self.config = config
        self.log = log
        self.msg = ''
        self.iterator = None
        self.param = None

    def init(self):
        items = [1]
        self.iterator = EnvItemIterator(items, None)
        items = [3, 4]
        self.iterator = EnvItemIterator(items, self.iterator)
        items = [None]
        self.iterator = EnvItemIterator(items, self.iterator)
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
        print 'exec %s' % self.param
        return True

    def clear(self):
        print 'clear %s' % self.param
        return True

    def get_info(self):
        return str(self.param) + '\n'

    def get_message(self):
        return self.msg
