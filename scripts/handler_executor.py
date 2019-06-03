#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import importlib
import sys
from executor import Executor


class HandlerExecutor(Executor):
    handlers = {}

    def __init__(self, target, path, log):
        super(HandlerExecutor, self).__init__(target, path, log)
        HandlerExecutor.find_handlers()

    @staticmethod
    def find_handlers():
        if len(HandlerExecutor.handlers):
            return

        package = importlib.import_module('test_handler')
        module_names = getattr(package, '__all__')
        for name in module_names:
            full_name = 'test_handler.' + name
            module = importlib.import_module(full_name)
            for attr in dir(module):
                if not attr.startswith('__'):
                    value = HandlerExecutor.handlers.get(attr, None)
                    if not value:
                        HandlerExecutor.handlers[attr] = full_name

    def execute(self):
        module_name = HandlerExecutor.handlers.get(self.target, None)
        if not module_name:
            self.msg = 'find module name of %s failed ' % self.target
            return False
        module = importlib.import_module(module_name)
        handler = getattr(module, self.target)(self.log)
        handler.execute()

        return True
