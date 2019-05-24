#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from env_base import EnvExecutorBase


class EnvColdSwap(EnvExecutorBase):
    def __init__(self, config, log):
        super(EnvColdSwap, self).__init__(config, log)
        print 'cold swap'

    def to_next(self):
        pass

    def real_execute(self):
        print 'exec cold swap'
        pass

    def real_clear(self):
        print 'clear cold swap'
        pass
