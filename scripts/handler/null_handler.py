#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from handler import Handler


class NullHandler(Handler):
    def __init__(self, context, path, log):
        super(NullHandler, self).__init__(context, path, log)

    def execute(self):
        return True
