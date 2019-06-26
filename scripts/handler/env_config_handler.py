#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import commands
import ConfigParser
from handler import Handler
from api.func import *


class EnvConfigHandler(Handler):
    def __init__(self, path, log):
        super(EnvConfigHandler, self).__init__(path, log)
        self.__back_up = False
        self.__hosts = None
        self.__config = {}

    def __parse_context(self):
        self.__config = {}
        for context in self.context:
            if not self.__hosts:
                if not context.get('hosts', None):
                    self.msg = 'EnvConfigHandler execute failed: hosts is None '
                    return False
                self.__hosts = context['hosts'].split(',')
            attr = context['attr']
            type = attr.get('type', None)
            section = attr.get('section', None)
            option = attr.get('option', None)
            if not type or not section or not option:
                self.msg = 'EnvConfigHandler execute failed: type/section/option not found '
                return False
            d = self.__config.get(type, None)
            key = section + ':' + option
            if d:
                d[key] = context['value']
            else:
                d = {key:context['value']}
                self.__config[type] = d
        return True

    def __get_config_name(self, key):
        name = None
        if key == 'gcluster':
            base = os.getenv('GCLUSTER_BASE')
            if not base:
                self.msg = 'EnvConfigHandler execute failed: env GCLUSTER_BASE not found'
            name = base + '/config/gbase_8a_gcluster.cnf'
        elif key == 'gbase':
            base = os.getenv('GBASE_BASE')
            if not base:
                self.msg = 'EnvConfigHandler execute failed: env GCLUSTER_BASE not found'
            name = base + '/config/gbase_8a_gbase.cnf'
        return name

    def __get_bak_name(self, name):
        path = file_base_dir(name)
        short_name = file_short_name(name)
        bak_name = path + '/test_bak_' + short_name
        return bak_name

    def __cpush(self, name):
        cmd = 'cpush %s %s' % (name, name)
        (status, output) = commands.getstatusoutput(cmd)
        if status != 0:
            self.msg = 'EnvConfigHandler execute failed: command %s failed' % cmd
            self.msg += 'output: %s' % output
            return False
        return True

    def __edit_config(self, config_name, param):
        bak_name = self.__get_bak_name(config_name)
        if os.path.isfile(bak_name):
            self.msg = 'EnvConfigHandler execute failed: bak conf (%s) exists, recover first' % bak_name
            return False
        shutil.copy(config_name, bak_name)
        parser = ConfigParser.RawConfigParser(allow_no_value=True)
        parser.read(config_name)
        for key in param.keys():
            s = key.split(':')
            section = s[0]
            option = s[1]
            parser.set(section, option, param.get(key))
        with open(config_name, 'w') as f:
            parser.write(f)
        if not self.__cpush(config_name):
            return False
        return True

    def execute(self):
        self.log.debug('EnvConfigHandler execute')
        if not self.__parse_context():
            return False
        self.__back_up = True
        for key in self.__config.keys():
            config_name = self.__get_config_name(key)
            if not config_name or not os.path.isfile(config_name):
                self.msg = 'EnvConfigHandler execute failed: config file(%s) not found ' % config_name
                return False
            # todo: use ssh to each host
            # for host in self.__hosts:
            #     self.__edit_config(host, config_name, self.__config.get(key))
            if not self.__edit_config(config_name, self.__config.get(key)):
                return False
        return True

    def clear(self):
        self.log.debug('EnvConfigHandler clear')
        if not self.__back_up:
            self.msg = 'EnvConfigHandler clear failed: no back up for clear '
            return False
        for key in self.__config.keys():
            config_name = self.__get_config_name(key)
            bak_name = self.__get_bak_name(config_name)
            if not bak_name or not os.path.isfile(bak_name):
                self.msg = 'EnvConfigHandler clear failed: bak config file(%s) not found ' % bak_name
                return False
            os.remove(config_name)
            shutil.move(bak_name, config_name)
            if not self.__cpush(config_name):
                return False
            # todo: use ssh to each host
        return True
