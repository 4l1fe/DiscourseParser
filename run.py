from datetime import datetime, timezone
from dataclasses import asdict
from time import sleep
from argparse import ArgumentParser
from typing import Union
from threading import Thread
from urllib.parse import urlencode, urlparse, parse_qsl
from itertools import chain
from queue import Empty

import structlog
from structlog.contextvars import bind_contextvars as bind_log_ctx, \
                                  bound_contextvars as bound_ctx
from httpx import Limits, Timeout, Client

from containers import ParserLatestArgs, ParserTopicArgs, CmdLatestParams
from storage import TargetDir
from synch import StopEvent, TaskExchanger
from constants import EventsEnum, FORUM_HOST, LATEST_URL, TOPIC_URL, REQUEST_DELAY


structlog.configure(
    wrapper_class=structlog.BoundLogger,
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
       

class Parser:
    TARGET_LATEST = 'latest'
    TARGET_TOPIC = 'topic'
    
    def __init__(self, client, target_dir: TargetDir, stop_event: StopEvent,
                 task_exchanger: TaskExchanger):
        self.client = client
        self.target_dir = target_dir
        self.stop_event = stop_event
        self.task_exchanger = task_exchanger

    def parse_target(self, target):
        bind_log_ctx(target=target)
        logger.msg(EventsEnum.do.value)

        target_method = self.get_save_topic 
        get_args_method = self.task_exchanger.get_topic
        if target == self.TARGET_LATEST:
            target_method = self.get_save_latest
            get_args_method = self.task_exchanger.get_latest
            
        while not self.stop_event.is_set():
            try:
                args = get_args_method()
            except Empty:
                continue  # Do getting args or leave

            with bound_ctx(args=asdict(args)):
                sleep(REQUEST_DELAY)
                target_method(args)

        logger.msg(EventsEnum.done.value)

    def get_save_latest(self, args: ParserLatestArgs):
        data = self._get_data(self.TARGET_LATEST, args)
               
        if not data or self._is_blank_response(data):
            self.stop_event.found_blank()
            return  # return passes logging

        page = self._extract_current_page(args.url)
        self.target_dir.write_latest(page, data)

        new_url = self._extract_latest_url(data)
        l_args = ParserLatestArgs(url=new_url)
        self.task_exchanger.put_latest(l_args)  # Let other producers work ASAP

        for topic in self._extract_topics(data):
            t_args = ParserTopicArgs(page=page, id=topic['id'])
            self.task_exchanger.put_topic(t_args)

    def get_save_topic(self, args: ParserTopicArgs):        
        data = self._get_data(self.TARGET_TOPIC, args)

        if not data:
            return
            
        self.target_dir.write_topic(args.page, data, args.id)

    def _is_blank_response(self, data: dict):
        if not self._extract_latest_url(data) or not self._extract_topics(data):
            return True

        return False

    def _get_data(self, target, args) -> Union[dict, None]:
        if target == self.TARGET_TOPIC: # TODO replace with args isinstance() check
            url = TOPIC_URL.format(id=args.id)  # target TARGET_TOPIC
        elif target == self.TARGET_LATEST:
            url = args.url  # take as is in response

        try:
            logger.msg(EventsEnum.data_get.value)
            resp = self.client.get(url)
        except Exception as e:
            logger.msg(EventsEnum.error_connection.value, exc_info=e)
            return

        try:
            resp.raise_for_status()
            data = resp.json()
            logger.msg(EventsEnum.data_received.value)
            return data
        except Exception as e:
            logger.msg(EventsEnum.error_http.value, exc_info=e)
            return

    def _extract_latest_url(self, data):
        return data.get('topic_list', {}).get('more_topics_url', None)

    def _extract_topics(self, data):
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
    args = ParserLatestArgs(url=LATEST_URL + '?' + query_params)
    task_exchanger.put_latest(args)

    # Run pools
    prod_pool = [Thread(target=parser.parse_target,
                        args=(parser.TARGET_LATEST, ),
                        name=f'Producer-{i}',
                        daemon=True)
                 for i in range(1, producers_n + 1) ] 
    cons_pool = [Thread(target=parser.parse_target,
                        args=(parser.TARGET_TOPIC, ),
                        name=f'Consumer-{i}',
                        daemon=True)
                 for i in range(1, consumers_n + 1) ] 

    # Let the consumerts get started first
    for thread in chain(cons_pool, prod_pool):
        thread.start()

    logger.msg('stop.wait')
    stop_event.wait()
        
    # Normal termination
    logger.msg('threads.join')
    for thread in chain(cons_pool, prod_pool):
        thread.join()

    logger.msg(EventsEnum.finished.value)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-cn', '--consumers', type=int, default=1)
    parser.add_argument('-pn', '--producers', type=int, default=1)

    subparsers = parser.add_subparsers(dest='command')

    latest_parser = subparsers.add_parser('latest')
    latest_parser.add_argument('-p', '--page', type=int, default=0)

    args = parser.parse_args()
    if args.command == 'latest':
        run_latest(consumers_n=args.consumers,
                   producers_n=args.producers,
                   start_page=args.page)
    
      
