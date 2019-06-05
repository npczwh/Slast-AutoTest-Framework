#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from handler import Handler


class EnvConfigHandler(Handler):
    def __init__(self, path, log):
        super(EnvConfigHandler, self).__init__(path, log)

    def execute(self):
        print 'EnvConfigHandler exec'
        return True

    def clear(self):
        print 'EnvConfigHandler clear'
        return True
