#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from abc import ABCMeta, abstractmethod


class Handler(object):
    __metaclass__ = ABCMeta

    def __init__(self, log):
        self.log = log
        self.msg = ''

    @abstractmethod
    def execute(self):
        pass

    def get_message(self):
        return self.msg

