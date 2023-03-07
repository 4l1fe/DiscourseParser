import json
from pathlib import Path
from enum import Enum
from datetime import datetime, timezone
from threading import RLock, Event
from dataclasses import dataclass, asdict
from time import sleep
from argparse import ArgumentParser
from signal import signal
from typing import Union
from queue import SimpleQueue
from multiprocessing.pool import ThreadPool
from urllib.parse import urlencode, urlparse, parse_qsl

import structlog
from httpx import Limits, Timeout, Client


structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt='iso'),
        structlog.processors.CallsiteParameterAdder(
            parameters=[structlog.processors.CallsiteParameter.THREAD_NAME, ]
        ),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=False
)
logger = structlog.get_logger()

FORUM_HOST = 'https://discuss.streamlit.io'
LATEST_URL = '/latest'
TOPIC_URL = '/t/{id}'


class EventsEnum(Enum):
    error_connection = 'error.connection'
    error_http = 'error.http'
    error_file = 'error.file'
    data_received = 'data.received'
    data_saved = 'data.saved'
    started = 'started'
    do = 'do'
    done = 'done'
    finished = 'finished'
    stop_signal = 'stop.signal'
    stop_blank = 'stop.blank'


@dataclass
class CmdLatestParams:
    order: str = 'created'
    ascending: bool = False
    page: int = 0


@dataclass
class ParserLatestArgs:
    more_url: str = ''


@dataclass
class ParserTopicArgs:
    page: int
    id: int
    

class StopEvent:

    def __init__(self, event):
        self.event = Event()
    
    def received_signal(self): #TODO
        self.event.set()
        logger.msg(EventsEnum.stop_signal.value, signal='signal received...')

    def found_blank(self, url):
        self.event.set()
        logger.msg(EventsEnum.stop_blank.value, url=url)

    def is_set(self):
        return self.event.is_set()
        

class TaskExchanger:

    def __init__(self):
        self.topic_queue = SimpleQueue()
        self.latest_queue = SimpleQueue()

    def get_topic(self):
        self.topic_queue.get()

    def put_topic(self, id_):
        self.topic_queue.put(id_)

    def put_latest(self, url):
        self.latest_queue.put(url)

    def get_latest(self):
        self.latest_queue.get()
        

class TargetDir:
    TARGET_LATEST = 1
    TARGET_TOPIC = 2
    
    def __init__(self, method: str, params: CmdLatestParams, start_iso_date: str):
        self.start_iso_date = start_iso_date
        self.target_dir = self._create_target_dir(method, params)
        self.rlock = RLock()
    
    def write_latest(self, page, data):
        self._write_data(self.TARGET_LATEST, page, data)

    def write_topic(self, page, data, topic_id):
        self._write_data(self.TARGET_TOPIC, page, data, topic_id)

    def get_latest_path(self) -> Path:
        p = self.target_dir / f'latest-{self.start_iso_date}.json'
        return p

    def get_topic_path(self, topic_id) -> Path:
        p = self.target_dir / f'topic-{topic_id}-{self.start_iso_date}.json'
        return p

    def _create_target_dir(self, method, params):
        '''Subdirs order derives from the Params class attributes order'''

        p = Path(method)
        parts = asdict(params)
        
        for key, value in parts.items():
            p = p / f'{key}-{value}'

        p.mkdir(parents=True, exist_ok=True)

        return p

    def _set_page(self, page):
        with self.rlock:
            if not page:
                page = 0  # The only case when 0 is first page?
            self.target_dir = self.target_dir.parent / f'page-{page}'
            self.target_dir.mkdir(exist_ok=True)
    
    def _write_data(self, target, page, data, *args):
        with self.rlock:
            self._set_page(page)
            text = json.dumps(data)
            get_path = self.get_topic_path
            if target == self.TARGET_LATEST:
                get_path = self.get_latest_path

            try:
                p = get_path(*args)
                p.write_text(text)        
                logger.msg(EventsEnum.data_saved.value, file=p.as_posix())
            except Exception as e:
                logger.msg(EventsEnum.error_file.value, exc_info=e,
                           path=self.target_dir.as_posix(), args=args)


class Parser:
    TARGET_LATEST = 1
    TARGET_TOPIC = 2
    
    def __init__(self, client, target_dir: TargetDir, stop_event: StopEvent,
                 task_exchanger: TaskExchanger):
        self.client = client
        self.target_dir = target_dir
        self.stop_event = stop_event
        self.task_exchanger = task_exchanger

    def get_save_latest(self):
        logger.msg(EventsEnum.do.value)

        while not self.stop_event.is_set():
            args: ParserLatestArgs = self.task_exchanger.get_latest()

            data = self._get_data(self, self.TARGET_LATEST, args)
                   
            if not data or self._is_blank_response(data):
                self.stop_event.found_blank(args.url)
                continue  # return pass logging

            page = self._extract_current_page(args.url)
            self.target_dir.write_latest(page, data)

            new_url = self._extract_latest_url(data)
            l_args = ParserLatestArgs(url=new_url)
            self.task_exchanger.put_latest(l_args)  # Let other producers work ASAP

            for topic in self._get_topics(data):
                t_args = ParserTopicArgs(page=page, id=topic['id'])
                self.task_exchanger.put_topic(t_args)

        logger.msg(EventsEnum.done.value)
        
    def get_save_topic(self):
        logger.msg(EventsEnum.do.value)

        while not self.stop_event.is_set():
            args: ParserTopicArgs = self.task_exchanger.get_topic()
            
            data = self._get_data(self, self.TARGET_TOPIC, args)

            if not data:
                continue
                
            self.target_dir.write_topic(args.page, data, args.id)

        logger.msg(EventsEnum.done.value)

    def _is_blank_response(self, data: dict):
        if not self._extract_latest_url(data) or not self._get_topics(data):
            return True

        return False

    def _get_data(self, target, args) -> Union[dict, None]:
        if target == self.TARGET_TOPIC: # TODO replace with args isinstance() check
            url = TOPIC_URL.format(id=args.id)  # target TARGET_TOPIC
        elif target == self.TARGET_LATEST:
            url = args.url  # take as is in response

        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.msg(EventsEnum.error_connection.value, exc_info=e)
            return

        try:
            resp.raise_for_status()
            data = resp.json()
            logger.msg(EventsEnum.data_received.value, url=url)
            return data
        except Exception as e:
            logger.msg(EventsEnum.error_http.value, exc_info=e)
            return

    def _extract_latest_url(self, data):
        return data.get('topic_list', {}).get('more_topics_url', None)

    def _get_topics(self, data):
        return data.get('topic_list', {}).get('topics', None)

    def _extract_current_page(self, url):
        qs_params = dict(parse_qsl(urlparse(url).query))
        return qs_params.get('page')


def run_latest(consumers_n=1, producers_n=1,  start_page: int = 0):
    logger.msg(EventsEnum.started.value)

    # Initialization
    start_iso_date = datetime.now(timezone.utc).isoformat(timespec='seconds')
    method = 'latest'
    timeout = Timeout(15.0)
    client = Client(base_url=FORUM_HOST, headers={'accept': 'application/json'},
                    limits=Limits(max_connections=consumers_n + producers_n,
                                  max_keepalive_connections=consumers_n + producers_n,
                                  keepalive_expiry=5),
                    timeout=timeout)
    stop_event = StopEvent()
    params = CmdLatestParams(page=start_page)
    target_dir = TargetDir(method, params, start_iso_date)
    task_exchanger = TaskExchanger()
    parser = Parser(client, target_dir, stop_event, task_exchanger)

    # Create and enqueue a start url
    query_params = asdict(params)
    if params.page == 0:  # First page without param
        query_params.pop('page')
    query_params = urlencode(query_params)
    start_url = LATEST_URL + '?' + query_params
    task_exchanger.put_latest(start_url)

    # Run pools
    with ThreadPool(producers_n, parser.get_save_latest) as prod_pool, \
            ThreadPool(consumers_n, parser.get_save_topic) as cons_pool:
        stop_event.wait()

    logger.msg(EventsEnum.finished.value)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--consumers', type=int, default=1)
    parser.add_argument('--producers', type=int, default=1)

    subparsers = parser.add_subparsers(dest='command')

    latest_parser = subparsers.add_parser('latest')
    latest_parser.add_argument('-p', '--page', type=int, default=0)

    args = parser.parse_args()
    if args.command == 'latest':
        run_latest(consumers_n=args.consumers,
                   producers_n=args.producers,
                   start_page=args.page)
        
      
