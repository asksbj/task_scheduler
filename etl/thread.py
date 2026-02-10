import threading
from typing import Callable

class TaskThread(threading.Thread):
    def __init__(self, task_name: str, task_function: Callable, *args, **kwargs):
        super().__init__()
        self.task_name = task_name
        self.task_function = task_function
        self.args = args
        self.kwargs = kwargs

    def run(self):
        self.task_function(*self.args, **self.kwargs)

    def keep_alive(self):
        pass