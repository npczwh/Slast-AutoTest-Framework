#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from handler import Handler


class EnvServiceHandler(Handler):
    def __init__(self, path, log):
        super(EnvServiceHandler, self).__init__(path, log)
        self.__back_up = False

    def execute(self):
        print 'EnvServiceHandler exec'
        self.__back_up = True
        return True

    def clear(self):
        if not self.__back_up:
            self.msg = 'EnvServiceHandler clear failed: no back up for clear '
            return False
        print 'EnvServiceHandler clear'
        return True
