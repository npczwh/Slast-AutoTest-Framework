#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from abc import ABCMeta, abstractmethod


class EnvExecutorBase:
    __metaclass__ = ABCMeta

    def __init__(self, config, log):
        self.config = config
        self.log = log
        self.index = 0
        self.size = 1
        self.msg = ''

    def init(self):
        return True

    def to_next(self):
        if self.index < self.size:
            self.index += 1
            return True
        else:
            return False

    def is_last(self):
        return self.index == self.size

    @abstractmethod
    def execute(self):
        return False

    @abstractmethod
    def clear(self):
        return False

    def reset(self):
        self.index = 0

    def get_message(self):
        return self.msg
