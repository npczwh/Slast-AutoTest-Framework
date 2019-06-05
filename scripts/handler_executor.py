#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import importlib
from executor import Executor


class HandlerExecutor(Executor):
    HANDLER_MODULE = 'handler'

    handlers = {}

    def __init__(self, target, path, log):
        super(HandlerExecutor, self).__init__(target, path, log)
        HandlerExecutor.find_handlers()

    @staticmethod
    def find_handlers():
        if len(HandlerExecutor.handlers):
            return

        package = importlib.import_module(HandlerExecutor.HANDLER_MODULE)
        module_names = getattr(package, '__all__')
        for name in module_names:
            full_name = HandlerExecutor.HANDLER_MODULE + '.' + name
            module = importlib.import_module(full_name)
            for attr in dir(module):
                if not attr.startswith('__'):
                    value = HandlerExecutor.handlers.get(attr, None)
                    if not value:
                        HandlerExecutor.handlers[attr] = full_name

    def execute(self):
        handler = self.create_handler()
        if not handler:
            return False
        handler.set_context(self.context)
        if not handler.execute():
            self.msg = handler.get_message()
            return False
        return True

    def create_handler(self):
        module_name = HandlerExecutor.handlers.get(self.target, None)
        if not module_name:
            self.msg = 'find module name of %s failed ' % self.target
            return None
        module = importlib.import_module(module_name)
        handler = getattr(module, self.target)(self.path, self.log)
        return handler
