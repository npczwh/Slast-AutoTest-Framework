#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from handler import Handler


class CompareFileHandler(Handler):
    def __init__(self, log):
        super(CompareFileHandler, self).__init__(log)
