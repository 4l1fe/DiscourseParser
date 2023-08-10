import signal
from threading import Event
from queue import SimpleQueue
from typing import Union

import structlog

from constants import EventsEnum, TASK_CHECK_TIMEOUT, TargetsEnum


logger = structlog.get_logger()


class StopEvent:

    def __init__(self):
        self.all_stopped = Event()
        self.producers_stopped = Event()
        self.consumers_stopped = Event()
        self._set_signal_handlers()

    def handle_terminate(self, sig, frame):
        self._handle_signal(sig, frame)

    def handle_keyboard_intr(self, sig, frame):
        self._handle_signal(sig, frame)
        
    def found_blank(self):
        self.producers_stopped.set()
        logger.msg(EventsEnum.stop_blank.value)

    def consumed_all(self, target):
        if target != TargetsEnum.TOPIC:
            return
        
        if self.producers_stopped.is_set():
            self.consumers_stopped.set()

    def is_set(self, target: TargetsEnum):
        if self.all_stopped.is_set():
            return True
        
        if target == TargetsEnum.LATEST:
            return self.producers_stopped.is_set()

        if target == TargetsEnum.TOPIC:
            return self.consumers_stopped.is_set()
                    
    def _handle_signal(self, sig_num, frame):
        if sig_num == signal.SIGINT:
            self.producers_stopped.set()
        elif sig_num == signal.SIGTERM:
            self.all_stopped.set()
            
        sig_name = signal.Signals(sig_num).name
        logger.msg(EventsEnum.stop_signal.value, signal=sig_name)

    def _set_signal_handlers(self):
        signal.signal(signal.SIGINT, self.handle_keyboard_intr)
        signal.signal(signal.SIGTERM, self.handle_terminate)

        
class TaskExchanger:

    def __init__(self):
        self.topic_queue = SimpleQueue()
        self.latest_queue = SimpleQueue()

    def get_topic(self):
        logger.msg(EventsEnum.task_get.value)
        return self.topic_queue.get(timeout=TASK_CHECK_TIMEOUT)

    def put_topic(self, args):
        self.topic_queue.put(args)

    def put_latest(self, args):
        self.latest_queue.put(args)

    def get_latest(self):
        logger.msg(EventsEnum.task_get.value)
        return self.latest_queue.get(timeout=TASK_CHECK_TIMEOUT)
