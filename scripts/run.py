#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import sys
import os
import re
from test_framework import TestFramework


def read_file(filename):
    with open(filename) as f:
        return f.read()


def version():
    filename = '%s\\__init__.py' % os.getcwd()
    return re.search("__version__ = '([0-9.]*)'", read_file(filename)).group(1)


def print_usage():
    print 'Usage: python run.py arg1'
    print '1. python run.py <config> : Read config file and run test'
    print '2. python run.py -h,--help : Print help information and exit'
    print '3. python run.py -v,--version : Print version information and exit'

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'Invalid param number. Please read the usage below. \n'
        print_usage()
    elif sys.argv[1] == '-h' or sys.argv[1] == '--help':
        print_usage()
    elif sys.argv[1] == '-v' or sys.argv[1] == '--version':
        print version()
    elif not os.path.isfile(sys.argv[1]):
        print 'Invalid param. Please read the usage below. \n'
        print_usage()
    else:
        test = TestFramework(sys.argv[1])
        test.run()
