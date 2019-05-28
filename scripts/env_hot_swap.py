#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from env_base import EnvExecutorBase


class EnvHotSwap(EnvExecutorBase):
    def __init__(self, config, log):
        super(EnvHotSwap, self).__init__(config, log)
        self.index = 0
        print 'hot swap'

    def to_next(self):
        if self.index == 0:
            self.index = 1
            return True
        return False

    def real_execute(self):
        print 'exec hot swap'
        return True

    def real_clear(self):
        print 'clear hot swap'
        return True
