#!/usr/bin/env python
# _*_ coding: utf-8 _*_


class EnvStep(object):
    def __init__(self, executor):
        self.__single_child = None
        self.__executor = executor
        self.__msg = ''

    def to_next(self):
        if self.has_child():
            if self.__single_child.to_next():
                return True
        if self.__executor.to_next():
            return True
        else:
            self.__executor.reset()
            return False

    def add_descendant(self, des):
        if self.has_child():
            self.__single_child.add_descendant(des)
        else:
            self.__single_child = des

    def has_child(self):
        if self.__single_child:
            return True
        else:
            return False

    def execute(self):
        if not self.__executor.execute():
            self.__msg += self.__executor.get_message()
            return False
        if self.has_child():
            if not self.__single_child.execute():
                self.__msg += self.__executor.get_message()
                return False
        return True

    def clear(self):
        if self.has_child():
            if not self.__single_child.clear():
                self.__msg += self.__single_child.get_message()
                return False
        if not self.__executor.clear():
            self.__msg += self.__executor.get_message()
            return False
        return True

    def get_and_clear_message(self):
        msg = self.__msg
        self.__msg = ''
        return msg
