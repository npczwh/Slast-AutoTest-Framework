#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import logging
import ConfigParser
import xml.etree.ElementTree as ET
from func import *
from env_step import EnvStep
from env_adapter import EnvAdapter
from list_reader import ListReader
from execute_step_factory import ExecuteStepFactory


class TestFramework(object):
    FULL = 1
    NORMAL = 2
    SMOKE = 3

    STRICT = 1
    TOLERATE = 2

    # todo: add param to create expect only
    def __init__(self, path, level_name):
        self.__path = path
        self.__config = path + '/conf/' + file_short_name(self.__path) + '.conf'
        self.__env_step = None
        self.__executor_list = []
        self.__level = self.__level_num(level_name)
        self.__mode = None
        self.__log = None
        formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(message)s')
        # todo: log path, log level
        handler = logging.FileHandler('tmp.log')
        handler.setFormatter(formatter)
        self.__log = logging.getLogger()
        self.__log.addHandler(handler)
        self.__log.setLevel(logging.INFO)
        self.__msg = ''
        self.__env_index = 0

    def get_name(self):
        return file_short_name(self.__path)

    def get_message(self):
        return self.__msg

    def __parse_conf(self):
        if not os.path.isfile(self.__config):
            self.__msg = 'config file (%s) not found. ' % self.__config
            return False
        self.__parser = ConfigParser.SafeConfigParser()
        self.__parser.read(self.__config)
        level = self.__parser.get('suite', 'test_level').strip()
        if level:
            self.__level = self.__level_num(level)
        self.__mode = int(self.__parser.get('suite', 'test_mode').strip())
        return True

    def __add_one_env(self, name):
        conf_path = file_base_dir(self.__config)
        config = real_file_name(conf_path, self.__parser.get(name, 'config'))
        skip = self.__parser.get(name, 'skip').strip()
        if skip:
            if int(skip):
                return True
        if not config:
            self.__msg = '%s config (%s) not found. ' % (name, self.__parser.get(name, 'config'))
            return False
        env = EnvAdapter(name, config, self.__path, self.__log)
        self.__env_step = EnvStep(env, self.__env_step)
        return True

    def __create_env_steps(self):
        if not self.__add_one_env('cold swap'):
            return False
        if not self.__add_one_env('hot swap'):
            return False
        return True

    def __add_one_normal_list(self, factory, list):
        level = self.__level_num('normal')
        for item in list:
            if item[0] == '[' and item[-1] == ']':
                item = item[1:-1:]
                level = self.__level_num(item)
                continue
            if level < self.__level:
                continue
            executor = factory.create_step(file_short_name(item), 'normal')
            if executor:
                self.__executor_list.append(executor)

    def __add_one_xml_list(self, factory, list):
        for item in list:
            level = self.NORMAL
            if item.get('level', None):
                level = self.__level_num(item['level'])
            if level < self.__level:
                continue
            executor = factory.create_step(item, 'xml')
            if executor:
                self.__executor_list.append(executor)

    def __add_one_list(self, factory, list, list_type):
        if list_type == 'xml':
            self.__add_one_xml_list(factory, list)
        else:
            self.__add_one_normal_list(factory, list)

    def __get_one_list(self, name):
        one_list = []
        list_type = file_suffix(name)
        if list_type == 'xml':
            tree = ET.parse(name)
            root = tree.getroot()
            for test in root:
                one_list.append(test.attrib)
        else:
            reader = ListReader()
            reader.set_base_path(self.__path + '/case')
            if reader.readfile(name):
                one_list = reader.get_list()
            else:
                self.__msg = reader.get_message()
        return one_list

    def __create_executors(self):
        conf_path = file_base_dir(self.__config)
        whitelist_name = real_file_name(conf_path, self.__parser.get('suite', 'whitelist'))
        blacklist_name = real_file_name(conf_path, self.__parser.get('suite', 'blacklist'))
        begin_at = self.__parser.get('suite', 'begin_at').strip()

        factory = ExecuteStepFactory()
        if not factory.init(whitelist_name, blacklist_name, begin_at, self.__path, self.__log):
            self.__msg = factory.get_message()
            return False

        list_files = line_to_list(self.__parser.get('suite', 'list'))
        for f in list_files:
            name = real_file_name(conf_path, f)
            list_type = file_suffix(name)
            one_list = self.__get_one_list(name)
            if not len(one_list):
                return False
            self.__add_one_list(factory, one_list, list_type)

        if not len(self.__executor_list):
            self.__msg = 'the real test list is empty '
            return False
        return True

    def __get_execute_ret(self, executor, type):
        ret = False
        if type == 'prepare':
            ret = executor.prepare()
        elif type == 'excute':
            ret = executor.excute()
        elif type == 'compare':
            ret = executor.compare()
        elif type == 'clear':
            ret = executor.clear()
        if not ret:
            print '%s --------- fail' % executor.get_name()
            self.__log.error('%s failed' % executor.get_name())
            self.__log.error('%s %s failed: ' % (type, executor.get_name()))
            self.__log.error(executor.get_message())
        if not ret and self.__mode == self.STRICT and type != 'compare':
            return False
        else:
            return True

    def __prepare_path(self):
        src = self.__path + '/expect_all/env' + str(self.__env_index)
        des = self.__path + '/expect'
        copy_path(src, des)
        re_mkdir(self.__path + '/result')

    def __save_result(self):
        src = self.__path + '/result'
        des = self.__path + '/result_all/env' + str(self.__env_index)
        mov_path(src, des)
        env_file = des + '/env_info'
        info = self.__env_step.get_info()
        write_file(env_file, 'w', '')
        for line in info:
            write_file(env_file, 'a', str(line) + '\n')

    def __execute(self):
        for executor in self.__executor_list:
            if not self.__get_execute_ret(executor, 'prepare'):
                return False
            if not self.__get_execute_ret(executor, 'excute'):
                self.__get_execute_ret(executor, 'clear')
                return False
            if not self.__get_execute_ret(executor, 'compare'):
                self.__get_execute_ret(executor, 'clear')
                return False
            if not self.__get_execute_ret(executor, 'clear'):
                return False
            if executor.get_res():
                print '%s --------- success' % executor.get_name()
            executor.reset()
        return True

    def __level_num(self, name):
        if name.lower() == 'full':
            return self.FULL
        elif name.lower() == 'normal':
            return self.NORMAL
        elif name.lower() == 'smoke':
            return self.SMOKE
        else:
            return self.NORMAL

    def run(self):
        print 'run with ' + self.__config
        self.__log.debug('run with ' + self.__config)
        ret = True
        if not self.__parse_conf():
            return False
        if not self.__create_env_steps():
            return False
        if not self.__create_executors():
            return False
        if self.__env_step is None:
            return self.__execute()

        self.__env_step = self.__env_step.get_root()
        if not self.__env_step.init():
            self.__msg += self.__env_step.get_and_clear_message()
            return False
        while self.__env_step.to_next():
            self.__env_index += 1
            self.__prepare_path()
            if not self.__env_step.execute():
                self.__msg += self.__env_step.get_and_clear_message()
                if not self.__env_step.clear():
                    self.__msg += self.__env_step.get_and_clear_message()
                ret = False
                break
            print 'env: ' + str(self.__env_step.get_info())
            print 'sub test start'
            if self.__execute():
                self.__save_result()
            else:
                if not self.__env_step.clear():
                    self.__msg += self.__env_step.get_and_clear_message()
                ret = False
                self.__msg += self.__env_step.get_and_clear_message()
                break
            print 'sub test end'
            if not self.__env_step.clear():
                self.__msg += self.__env_step.get_and_clear_message()
                ret = False
                break
        return ret
