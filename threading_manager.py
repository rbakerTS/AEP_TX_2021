import os
import sys

from typing import Callable, Union
from threading import Thread
from queue import Queue
from dataclasses import dataclass
import time

import uuid

from concurrent.futures import ThreadPoolExecutor


# todo: limit you can add to run  so many threads at once
class ThreadingManager:
    def __init__(self):
        self.threads = dict()

    def create_thread(self, f: Callable, args: Union[None, list] = None, kwargs: Union[None, dict] = None,
                      identifier: str = ''):
        identifier if identifier else uuid.uuid4()
        self.threads[identifier] = {'thread': Thread(target=f, args=args if args else []), 'started': False,
                                    'kwargs': kwargs}

    def start_thread(self, identifier: str):
        self.threads[identifier]['started'] = True
        self.threads[identifier]['thread'].start()

    def start_all_threads(self):
        pass

    def join_threads(self):
        for key in self.threads:
            self.threads[key]['thread'].join()
            self.threads[key]['started'] = 'Finished'

    def clean_threads(self):
        self.threads = dict()

    def check_if_alive(self, item):
        return self.threads[item]['thread'].is_alive()

    def __getitem__(self, key):
        return self.threads[key]


if __name__ == '__main__':
    import time
    import random


    # simple test ******************************************************************************************************
    # def func(thread_name):
    #     print(f'I am in the thread called: {thread_name}')
    #     time.sleep(random.randint(1, 5))
    #     print(f'finished in thread: {thread_name}')

    class Progress:
        def __init__(self, title, total):
            self._title = title
            self._total = total
            self._current = 0

        def update(self, current):
            self._current = current

        def __repr__(self):
            return f'{self._title}: {self._current}/{self._total}'

        def __str__(self):
            return f'{self._title}: {self._current}/{self._total}'


    class Printer:
        def __init__(self, progresses):
            self.progresses = progresses

        def __repr__(self):
            return '\t'.join([str(progress) for progress in self.progresses])


    def func(thread_name, items, progress:Progress):
        for index,_ in enumerate(items):
            progress.update(index+1)
            time.sleep(1)
        # print(f'done with thread: {thread_name}')

    def printer_job(printer:Printer):
        flag = True
        while flag:
            progresses = printer.progresses
            flag = False

            for progress in progresses:
                if progress._current != progress._total:
                    flag = True
            sys.stdout.write(f'\r{printer}')
            sys.stdout.flush()
            time.sleep(1)
            # os.system('cls||clear')

    name1 = 'thread1'
    name2 = 'thread2'
    name3 = 'thread3'

    item1 = [1] * random.randint(1, 10)
    item2 = [1] * random.randint(1, 10)
    item3 = [1] * random.randint(1, 10)

    p1 = Progress(name1, len(item1))
    p2 = Progress(name2, len(item2))
    p3 = Progress(name3, len(item3))

    printer = Printer([p1, p2, p3])

    args1 = [name1, item1, p1]
    args2 = [name2, item2, p2]
    args3 = [name3, item3, p3]

    t = ThreadingManager()
    t.create_thread(func, args=args1, identifier=name1)
    t.create_thread(func, args=args2, identifier=name2)
    t.create_thread(func, args=args3, identifier=name3)
    t.create_thread(printer_job,args=[printer],identifier='printer_thread')

    t.start_thread('thread1')
    t.start_thread('thread2')

    t.start_thread('printer_thread')

    time.sleep(10)
    t.start_thread('thread3')


    t.join_threads()
