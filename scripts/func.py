#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import os


def read_file(filename):
    with open(filename) as f:
        return f.read()


def file_base_dir(filename):
    base_dir = None
    if os.path.isfile(filename):
        base_dir = os.path.abspath(os.path.dirname(filename))
    return base_dir

if __name__ == '__main__':
    # test
    name = '%s\\func.py' % os.getcwd()
    print name
    print file_base_dir(name)
    exit()
