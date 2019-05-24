#!/usr/bin/env python
# _*_ coding: utf-8 _*_


class EnvStepBase(object):
    def __init__(self, config, log):
        self.config = config
        self.log = log
        self.msg = ''

    def to_next(self):
        return False

    def to_prev(self):
        return False

    def excute(self):
        return False

    def clear(self):
        return False

    def get_message(self):
        return self.msg
