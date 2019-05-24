#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import ConfigParser
from func import *
from env_cold_swap import EnvColdSwap
from env_hot_swap import EnvHotSwap


class TestFramework(object):
    def __init__(self, path, test_level):
        self.__name = file_short_name(path)
        self.__config = path + '\\conf\\' + self.__name + '.conf'
        self.__env_steps = []
        self.__log = None
        self.__test_level = test_level
        self.__msg = ''

    def get_name(self):
        return self.__name

    def get_message(self):
        return self.__msg

    def __parse_conf(self):
        if os.path.isfile(self.__config):
            self.__msg = 'config file (%s) not found' % self.__config
            return False
        self.__parser = ConfigParser.SafeConfigParser()
        self.__parser.read(self.__config)

    def __create_env_steps(self):
        skip = self.__parser.get('cold swap', 'skip')
        base_path = file_base_dir(self.__config)
        if not skip:
            config = real_file_name(base_path, self.__parser.get('cold swap', 'config'))
            step = EnvColdSwap(config, self.__log)
            self.__env_steps.append(step)
        skip = self.__parser.get('hot swap', 'skip')
        if not skip:
            config = real_file_name(base_path, self.__parser.get('hot swap', 'config'))
            step = EnvHotSwap(config, self.__log)
            self.__env_steps.append(step)

    def run(self):
        self.__create_env_steps()
        print 'run with ' + self.__config
        return False
