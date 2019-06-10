#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import commands
from abc import ABCMeta, abstractmethod


class Handler(object):
    __metaclass__ = ABCMeta

    def __init__(self, path, log):
        self.context = None
        self.path = path
        self.output = None
        self.log = log
        self.msg = ''

    @abstractmethod
    def execute(self):
        pass

    def clear(self):
        pass

    def set_context(self, context):
        self.context = context

    def get_message(self):
        return self.msg
