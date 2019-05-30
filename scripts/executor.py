#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from abc import ABCMeta, abstractmethod


class Executor(object):
    __metaclass__ = ABCMeta

    def __init__(self, target, path, log):
        self.target = target
        self.path = path
        self.log = log
        self.__handler = None
        self.__msg = ''

    @abstractmethod
    def execute(self):
        pass

    def get_message(self):
        return self.__msg