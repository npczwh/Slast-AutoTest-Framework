#!/usr/bin/env python
# _*_ coding: utf-8 _*_


# todo: refactoring with stack
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
            if self.has_child():
                self.__single_child.reset()
                self.to_next()
            return True
        else:
            return False

    def add_child(self, child):
        if not self.__single_child:
            self.__single_child = child

    def has_child(self):
        if self.__single_child:
            return True
        else:
            return False

    def reset(self):
        if self.has_child():
            self.__single_child.reset()
        self.__executor.reset()

    def need_clear(self):
        self.__should_clear = True

    def get_root(self):
        if self.__parent:
            return self.__parent.get_root()
        else:
            return self

    def init(self):
        if self.has_child():
            if not self.__single_child.init():
                self.__msg = self.__single_child.get_message()
                return False
        if self.__executor.init():
            if self.has_child():
                self.__executor.to_next()
            return True
        else:
            self.__msg = self.__executor.get_message()
            return False

    def execute(self):
        if not self.has_child() or self.__should_execute:
            if not self.__executor.execute():
                self.__msg += self.__executor.get_message() + '\n'
                return False
            self.__should_execute = False
        if self.has_child():
            if not self.__single_child.execute():
                self.__msg += self.__single_child.get_message() + '\n'
                return False
        return True

    def clear(self):
        if self.has_child():
            if not self.__single_child.clear():
                self.__msg += self.__single_child.get_message() + '\n'
                return False
        if not self.has_child() or self.__should_clear:
            if not self.__executor.clear():
                self.__msg += self.__executor.get_message() + '\n'
                return False
            self.__should_clear = False
        return True

    def get_info(self):
        info = self.__executor.get_info()
        if self.has_child():
            info += self.__single_child.get_info()
        return info

    def get_and_clear_message(self):
        msg = self.__msg
        self.__msg = ''
        return msg
