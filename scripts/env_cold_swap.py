#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from env_base import EnvExecutorBase
from env_item_iterator import EnvItemIterator


class EnvColdSwap(EnvExecutorBase):
    def __init__(self, config, log):
        super(EnvColdSwap, self).__init__(config, log)
        self.size = 1

    def execute(self):
        items = [1, 2]
        it = EnvItemIterator(items, None)
        items = [3, 4]
        it = EnvItemIterator(items, it)
        items = [None]
        it = EnvItemIterator(items, it)
        it = it.get_root()
        while (it.has_next()):
            print it.next([])
        print 'exec cold swap'
        return True

    def clear(self):
        print 'clear cold swap'
        return True
