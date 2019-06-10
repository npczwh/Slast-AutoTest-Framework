#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from handler import Handler


class NullHandler(Handler):
    def __init__(self, path, log):
        super(NullHandler, self).__init__(path, log)

    def execute(self):
        return True

    def clear(self):
        return True
