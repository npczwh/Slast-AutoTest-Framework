#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from env_base import EnvExecutorBase
from env_item_iterator import EnvItemIterator


class EnvColdSwap(EnvExecutorBase):
    def __init__(self, config, log):
        super(EnvColdSwap, self).__init__(config, log)
        self.iterator = None
        self.param = None
        self.size = 1

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
