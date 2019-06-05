#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import time
import commands
from handler import Handler


class EnvServiceHandler(Handler):
    def __init__(self, path, log):
        super(EnvServiceHandler, self).__init__(path, log)
        self.__hosts = None
        self.__config = {}

    def __parse_context(self):
        self.__config = {}
        for context in self.context:
            if not self.__hosts:
                if not context.get('hosts', None):
                    self.msg = 'EnvServiceHandler execute failed: hosts is None '
                    return False
                self.__hosts = context['hosts'].split(',')
            attr = context['attr']
            type = attr.get('type', None)
            if not type:
                self.msg = 'EnvServiceHandler execute failed: type not found '
                return False
            if not self.__config.get(type, None):
                self.__config[type] = context['value']
        return True

    def __do_command(self, cmd):
        (status, output) = commands.getstatusoutput(cmd)
        if status != 0:
            self.msg = 'EnvServiceHandler execute failed: command %s failed' % cmd
            self.msg += 'output: %s' % output
            return False
        return True

    def execute(self):
        self.log.debug('EnvServiceHandler exec')
        if not self.__parse_context():
            return False
        keys = self.__config.keys()
        for i in range(len(keys) - 1, -1, -1):
            # todo: use ssh instead
            cmd = 'cexec gcluster_services %s %s' % (keys[i], self.__config.get(keys[i]))
            if not self.__do_command(cmd):
                return False
        time.sleep(2)
        if not self.__do_command("gcadmin"):
            return False
        return True

    def clear(self):
        self.log.debug('EnvServiceHandler clear')
        return self.execute()
