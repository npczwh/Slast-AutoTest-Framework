#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import difflib
import os
import sys
from handler import Handler
sys.path.append('..')
from func import *


class CompareFileHandler(Handler):
    def __init__(self, path, log):
        super(CompareFileHandler, self).__init__(path, log)

    def execute(self):
        expect = self.path + '\\expect\\' + self.context + '.expect'
        result = self.path + '\\result\\' + self.context + '.result'

        if not os.path.isfile(expect) or not os.path.isfile(result):
            self.msg = 'expect (%s) or result (%s) is not exists ' % (expect, result)
            return False

        expect_str = read_lines(expect)
        result_str = read_lines(result)
        diff_list = list(difflib.ndiff(expect_str, result_str, charjunk=enter_junk))
        ret = True
        for line in diff_list:
            if line.startswith('-') or line.startswith('+'):
                ret = False
                break
        if not ret:
            diff = self.path + '\\result\\' + self.context + '.diff'
            buf = ''.join(diff_list)
            write_file(diff, 'w', buf)
        return ret


def enter_junk(c, ws="\n"):
    return c in ws
