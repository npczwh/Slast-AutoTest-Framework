#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from func import *


class ListReader(object):
    def __init__(self):
        self.__msg = ''
        self.__lines = None

    def __target_exist(self, target, isfile):
        if isfile:
            return os.path.isfile(target)
        else:
            return os.path.exists(target)

    def readitem(self, filename):
        return self.__read(filename, False, False)

    def readfile(self, filename):
        return self.__read(filename, True, True)

    def readpath(self, filename):
        return self.__read(filename, False, True)

    def __read(self, filename, isfile, checkfile):
        self.__init__()
        lines = read_lines(filename)
        base_path = file_base_dir(filename)
        for i in range(len(lines) - 1, -1, -1):
            lines[i] = lines[i].strip()
            if not lines[i]:
                lines.remove(lines[i])
                continue
            if lines[i][0] is '#':
                lines.remove(lines[i])
                continue
            if checkfile:
                if not self.__target_exist(lines[i], isfile):
                    lines[i] = base_path + '\\' + lines[i]
                    if not self.__target_exist(lines[i], isfile):
                        self.__msg = 'config file %s in %s is not found.' % (lines[i], filename)
                        return False
                lines[i] = file_abs_name(lines[i])
        self.__lines = lines
        return True

    def get_message(self):
        return self.__msg

    def get_list(self):
        return self.__lines
