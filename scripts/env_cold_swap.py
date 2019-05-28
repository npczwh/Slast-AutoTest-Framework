#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from env_base import EnvExecutorBase


class EnvColdSwap(EnvExecutorBase):
    def __init__(self, config, log):
        super(EnvColdSwap, self).__init__(config, log)
        self.index = 0
        print 'cold swap'

    def to_next(self):
        if self.index == 0:
            self.index = 1
            return True
        return False

    def real_execute(self):
        print 'exec cold swap'
        return True

    def real_clear(self):
        print 'clear cold swap'
        return True
