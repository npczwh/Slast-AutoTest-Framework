#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import os
import ast
import shutil


def write_file(filename, mode, buf):
    with open(filename, mode) as f:
        return f.write(buf)


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


def file_base_name(filename):
    return file_short_name(filename).split(".")[0]


def file_abs_name(filename):
    return file_base_dir(filename) + '\\' + file_short_name(filename)


def file_suffix(filename):
    l = file_short_name(filename).split(".")
    if len(l) > 1:
        return l[-1]
    else:
        return ''


def real_file_name(base_path, filename):
    real_filename = filename.strip()
    if not os.path.isfile(real_filename):
        real_filename = base_path + '\\' + filename
        if not os.path.isfile(real_filename):
            real_filename = ''
    if real_filename:
        real_filename = file_abs_name(real_filename)
    return real_filename


def line_to_list(line):
    list = line.split(',')
    for i in range(len(list)):
        list[i] = list[i].strip()
    return list


def str_to_dict(s):
    d = None
    try:
        d = ast.literal_eval(s)
    except Exception as e:
        pass
    return d


def re_mkdir(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)


def mov_path(src, des):
    if os.path.exists(des):
        shutil.rmtree(des)
    shutil.move(src, des)


def copy_path(src, des):
    if os.path.exists(des):
        shutil.rmtree(des)
    shutil.copytree(src, des)


if __name__ == '__main__':
    # test
    name = '%s\\func.py' % os.getcwd()
    print name
    print file_suffix(name)
    exit()
