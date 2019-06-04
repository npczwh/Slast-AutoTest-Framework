#!/usr/bin/env python
# _*_ coding: utf-8 _*_


class EnvItemIterator(object):
    def __init__(self, name, type, items, parent):
        self.__name = name
        self.__type = type
        self.__items = items
        self.__parent = parent
        self.__child = None
        if self.__parent:
            self.__parent.set_child(self)
        self.__index = 0
        pass

    def set_child(self, child):
        self.__child = child

    def has_next(self):
        if self.__index < len(self.__items):
            if self.__child:
                if not self.__child.has_next():
                    return False
            return True
        else:
            return False

    def next(self, env):
        forward = True
        if self.__index < len(self.__items):
            env.append(self.__items[self.__index])
        else:
            env.append(None)
        if self.__child:
            env = self.__child.next(env)
            if self.__child.has_next():
                forward = False
            else:
                self.__child.reset()
        if forward:
            self.__index += 1
        return env

    def reset(self):
        self.__index = 0
        if self.__child:
            self.__child.reset()

    def get_env(self, env):
        if self.__index < len(self.__items):
            env.append(self.__items[self.__index])
        else:
            env.append(None)
        if self.__child:
            evn = self.__child.get_env(env)
        return env

    def get_root(self):
        if self.__parent:
            return self.__parent.get_root()
        else:
            return self
