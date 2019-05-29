#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from env_base import EnvExecutorBase


class EnvHotSwap(EnvExecutorBase):
    def __init__(self, config, log):
        super(EnvHotSwap, self).__init__(config, log)
        self.size = 2

    def execute(self):
        print 'exec hot swap'
        return True

    def clear(self):
        print 'clear hot swap'
        return True
