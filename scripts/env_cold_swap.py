#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from env_base import EnvExecutorBase


class EnvColdSwap(EnvExecutorBase):
    def __init__(self, config, log):
        super(EnvColdSwap, self).__init__(config, log)
        self.size = 1

    def execute(self):
        print 'exec cold swap'
        return True

    def clear(self):
        print 'clear cold swap'
        return True
