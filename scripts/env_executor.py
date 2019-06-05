#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from handler_executor import HandlerExecutor


class EnvExecutor(object):
    def __init__(self, handler_name, path, log):
        self.__handler = None
        self.__handler_name = handler_name
        self.__factory = HandlerExecutor(handler_name, path, log)
        self.__context = []
        self.__msg = ''

    def init(self):
        self.__handler = self.__factory.create_handler()
        if self.__handler:
            return True
        else:
            self.__msg = self.__factory.get_message()
            return False

    def execute(self):
        self.__handler.set_context(self.__context)
        if self.__handler.execute():
            return True
        else:
            self.__msg = self.__handler.get_message()
            return False

    def clear(self):
        if self.__handler.clear():
            return True
        else:
            self.__msg = self.__handler.get_message()
            return False

    def parse_conf(self, conf):
        for item in conf:
            param = item.get(self.__handler_name, None)
            if not param:
                continue
            self.__context.append(param)

    def get_message(self):
        return self.__msg
