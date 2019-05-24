#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from env_step_base import EnvStepBase


class EnvHotSwap(EnvStepBase):
    def __init__(self, config, log):
        super(EnvHotSwap, self).__init__(config, log)

    def to_next(self):
        return False

    def to_prev(self):
        return False

    def excute(self):
        return False

    def clear(self):
        return False