#!/usr/bin/env python
# _*_ coding: utf-8 _*_


class TestExecutor(object):
    def __init__(self, name):
        self.__msg = ''
        self.__name = name

    def get_name(self):
        return self.__name

    def prepare(self):
        pass

    def excute(self):
        pass

    def clear(self):
        pass

    def compare(self):
        pass

    def get_message(self):
        return self.__msg