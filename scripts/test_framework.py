#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import ConfigParser
from func import *
from env_step import EnvStep
from env_cold_swap import EnvColdSwap
from env_hot_swap import EnvHotSwap
from list_reader import ListReader
from test_executor_factory import TestExecutorFactory


class TestFramework(object):
    def __init__(self, path, level_name):
        self.__path = path
        self.__config = path + '\\conf\\' + file_short_name(self.__path) + '.conf'
        self.__env_step = None
        self.__executor_list = []
        self.__level = self.__level_num(level_name)
        self.__log = None
        self.__msg = ''

    def get_name(self):
        return file_short_name(self.__path)

    def get_message(self):
        return self.__msg

    def __parse_conf(self):
        if not os.path.isfile(self.__config):
            self.__msg += 'config file (%s) not found. ' % self.__config
            return False
        self.__parser = ConfigParser.SafeConfigParser()
        self.__parser.read(self.__config)
        return True

    def __add_one_env(self, type):
        conf_path = file_base_dir(self.__config)
        config = real_file_name(conf_path, self.__parser.get(type, 'config'))
        skip = self.__parser.get(type, 'skip')
        if skip:
            if int(skip):
                return True
        if not config:
            self.__msg += '%s config (%s) not found. ' % (type, self.__parser.get(type, 'config'))
            return False
        env = None
        if type == 'cold swap':
            env = EnvColdSwap(config, self.__log)
        elif type == 'hot swap':
            env = EnvHotSwap(config, self.__log)
        else:
            self.__msg += 'unsupported type (%s). ' % type
            return False
        if self.__env_step is None:
            self.__env_step = EnvStep(env)
        else:
            des = EnvStep(env)
            self.__env_step.add_descendant(des)
        return True

    def __create_env_steps(self):
        if not self.__add_one_env('cold swap'):
            return False
        if not self.__add_one_env('hot swap'):
            return False
        return True

    def __create_executors(self):
        conf_path = file_base_dir(self.__config)
        sql_list_name = real_file_name(conf_path, self.__parser.get('suite', 'sql_list'))
        customize_list_name = real_file_name(conf_path, self.__parser.get('suite', 'customize_list'))
        whitelist_name = real_file_name(conf_path, self.__parser.get('suite', 'whitelist'))
        blacklist_name = real_file_name(conf_path, self.__parser.get('suite', 'blacklist'))
        begin_at = self.__parser.get('suite', 'begin_at').strip()

        reader = ListReader()
        if not reader.readfile(sql_list_name):
            self.__msg += reader.get_message()
            return False
        sql_list = reader.get_list()

        factory = TestExecutorFactory()
        if not factory.init(whitelist_name, blacklist_name, begin_at):
            self.__msg += factory.get_message()
            return False


        for item in sql_list:
            # check level
            executor = factory.create_executor(file_base_name(item), None, None)
            if executor:
                self.__executor_list.append(executor)

        # if len(sql_list) is 0 or len(customize_list) is 0:
        #     self.__msg =
        #     return False
        return True

    def __execute(self):
        return True

    def __init_level(self):
        level = self.__level_num(self.__parser.get('suite', 'test_level').strip())
        if level:
            self.__level = level
        if not self.__level:
            self.__level = self.__level_num('normal')

    def __level_num(self, name):
        if not name:
            return 0
        elif name.lower() is 'full':
            return 1
        elif name.lower() is 'normal':
            return 2
        elif name.lower() is 'smoke':
            return 3
        else:
            return 2

    def run(self):
        print 'run with ' + self.__config
        ret = True
        if not self.__parse_conf():
            return False
        self.__init_level()
        if not self.__create_env_steps():
            return False
        if not self.__create_executors():
            return False
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
