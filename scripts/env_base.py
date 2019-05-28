#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from abc import ABCMeta, abstractmethod


class EnvExecutorBase:
    __metaclass__ = ABCMeta

    def __init__(self, config, log):
        self.config = config
        self.log = log
        self.__is_executed = False
        self.index = 0
        self.msg = ''

    @abstractmethod
    def to_next(self):
        pass

    @abstractmethod
    def real_execute(self):
        pass

    def execute(self):
        if self.__is_executed:
            return True
        if self.real_execute():
            self.__is_executed = True
            return True
        else:
            return False

    @abstractmethod
    def real_clear(self):
        pass

    def clear(self):
        if not self.__is_executed:
            return True
        if self.real_clear():
            self.__is_executed = False
            return True
        else:
            return False

    def reset(self):
        self.__is_executed = False
        self.index = 0

    def get_message(self):
        return self.msg
