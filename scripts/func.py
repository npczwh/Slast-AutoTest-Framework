#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import os


def read_file(filename):
    with open(filename) as f:
        return f.read()


def read_lines(filename):
    with open(filename) as f:
        return f.readlines()


def file_base_dir(filename):
    return os.path.abspath(os.path.dirname(filename))


def file_short_name(filename):
    return os.path.split(filename)[-1]


def file_abs_name(filename):
    return file_base_dir(filename) + '\\' + file_short_name(filename)


if __name__ == '__main__':
    # test
    name = '%s\\func' % os.getcwd()
    print name
    print file_short_name(name)
    exit()
