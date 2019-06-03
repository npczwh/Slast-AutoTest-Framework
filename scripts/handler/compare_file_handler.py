#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from handler import Handler


class CompareFileHandler(Handler):
    def __init__(self, context, log):
        super(CompareFileHandler, self).__init__(context, log)

    def execute(self):
        print 'CompareFileHandler execute '
        return True
