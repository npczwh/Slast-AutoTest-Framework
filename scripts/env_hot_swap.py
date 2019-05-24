#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from env_base import EnvExecutorBase


class EnvHotSwap(EnvExecutorBase):
    def __init__(self, config, log):
        super(EnvHotSwap, self).__init__(config, log)
        print 'hot swap'

    def to_next(self):
        pass

    def real_execute(self):
        print 'exec hot swap'
        pass

    def real_clear(self):
        print 'clear hot swap'
        pass
