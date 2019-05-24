#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import ConfigParser
from func import *
from env_step import EnvStep
from env_cold_swap import EnvColdSwap
from env_hot_swap import EnvHotSwap


class TestFramework(object):
    def __init__(self, path, test_level):
        self.__name = file_short_name(path)
        self.__config = path + '\\conf\\' + self.__name + '.conf'
        self.__env_step = None
        self.__log = None
        self.__test_level = test_level
        self.__msg = ''

    def get_name(self):
        return self.__name

    def get_message(self):
        return self.__msg

    def __parse_conf(self):
        if os.path.isfile(self.__config):
            self.__msg += 'config file (%s) not found. ' % self.__config
            return False
        self.__parser = ConfigParser.SafeConfigParser()
        self.__parser.read(self.__config)

    def __add_one_env(self, type):
        base_path = file_base_dir(self.__config)
        config = real_file_name(base_path, self.__parser.get(type, 'config'))
        skip = self.__parser.get(type, 'skip')
        if skip:
            pass
        env = None
        if type == 'cold swap':
            env = EnvColdSwap(config, self.__log)
        elif type == 'hot swap':
            env = EnvHotSwap(config, self.__log)
        if self.__env_step is None:
            self.__env_step = EnvStep(env)
        else:
            des = EnvStep(env)
            self.__env_step.add_descendant(des)

    def __create_env_steps(self):
        self.__add_one_env('cold swap')
        self.__add_one_env('hot swap')

    def __execute(self):
        return True

    def run(self):
        print 'run with ' + self.__config
        ret = True
        self.__create_env_steps()
        if self.__env_step is None:
            return self.__execute()
        while self.__env_step.to_next():
            if self.__env_step.execute():
                self.__execute()
            else:
                self.__msg += self.__env_step.get_and_clear_message()
            if not self.__env_step.clear():
                self.__msg += self.__env_step.get_and_clear_message()
                ret = False
                break
        return ret
