#!/usr/bin/env python
# _*_ coding: utf-8 _*_


class EnvStep(object):
    def __init__(self, executor, parent):
        self.__single_child = None
        self.__executor = executor
        self.__parent = parent
        if parent:
            parent.add_child(self)
        self.__should_execute = True
        self.__should_clear = False
        self.__msg = ''

    def to_next(self):
        if self.has_child():
            if self.__single_child.to_next():
                return True
        self.__should_execute = True
        if self.__executor.to_next():
            if self.__parent and self.__executor.is_last():
                self.__parent.need_clear()
            return True
        else:
            self.__executor.reset()
            self.to_next()
            return False

    def add_child(self, child):
        if not self.__single_child:
            self.__single_child = child
            self.__executor.to_next()

    def has_child(self):
        if self.__single_child:
            return True
        else:
            return False

    def need_clear(self):
        self.__should_clear = True

    def get_root(self):
        if self.__parent:
            return self.__parent.get_root()
        else:
            return self

    def execute(self):
        if not self.has_child() or self.__should_execute:
            if not self.__executor.execute():
                self.__msg += self.__executor.get_message()
                return False
            self.__should_execute = False
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
        if not self.has_child() or self.__should_clear:
            if not self.__executor.clear():
                self.__msg += self.__executor.get_message()
                return False
            self.__should_clear = False
        return True

    def get_and_clear_message(self):
        msg = self.__msg
        self.__msg = ''
        return msg
