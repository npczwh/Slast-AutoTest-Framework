#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from func import *


class ListReader(object):
    def __init__(self):
        self.__msg = ''
        self.__lines = None

    def read(self, filename):
        self.__lines = None
        lines = read_lines(filename)
        base_path = file_base_dir(filename)
        for i in range(len(lines)):
            lines[i] = lines[i].strip()
            if lines[i][0] is '#':
                lines.remove(lines[i])
                continue
            if not os.path.isfile(lines[i]):
                lines[i] = base_path + '\\' + lines[i]
                if not os.path.isfile(lines[i]):
                    self.__msg = 'config file %s in %s is not found.' % (lines[i], filename)
                    return False
            lines[i] = file_abs_name(lines[i])
        self.__lines = lines
        return True

    def get_message(self):
        return self.__msg

    def get_list(self):
        return self.__lines
