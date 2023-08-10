from datetime import datetime, timezone
from dataclasses import asdict
from typing import Union
from threading import Thread
from urllib.parse import urlencode, urlparse, parse_qsl
from itertools import chain
from queue import Empty

import structlog
from structlog.contextvars import bind_contextvars as bind_log_ctx, \
                                  bound_contextvars as bound_ctx
from httpx import Limits, Timeout, Client
from pyrate_limiter import Duration, RequestRate, Limiter

from containers import ParserLatestArgs, ParserTopicArgs, CmdLatestParams
from storage import TargetDir
from sync import StopEvent, TaskExchanger
from constants import EventsEnum, LATEST_URL, TOPIC_URL, TargetsEnum, RunDefaults


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
    
    def __init__(self, client, target_dir: TargetDir, stop_event: StopEvent,
                 task_exchanger: TaskExchanger, prods_limiter: Limiter,
                 cons_limiter: Limiter):
        self.client = client
        self.target_dir = target_dir
        self.stop_event = stop_event
        self.task_exchanger = task_exchanger
        self.prods_limiter = prods_limiter
        self.cons_limiter = cons_limiter

    def parse_target(self, target: TargetsEnum):
        bind_log_ctx(target=target.value)
        logger.msg(EventsEnum.do.value)

        target_method = self.get_save_topic 
        get_args_method = self.task_exchanger.get_topic
        if target == TargetsEnum.LATEST:
            target_method = self.get_save_latest
            get_args_method = self.task_exchanger.get_latest
            
        while not self.stop_event.is_set(target):
            try:
                args = get_args_method()
            except Empty:
                self.stop_event.consumed_all(target)
                continue  # Iterate next or leave

            with bound_ctx(args=asdict(args)):
                target_method(args)

        logger.msg(EventsEnum.done.value)

    def get_save_latest(self, args: ParserLatestArgs):
        data = self._get_data(TargetsEnum.LATEST, args)

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
        data = self._get_data(TargetsEnum.TOPIC, args)

        if not data:
            return
            
        self.target_dir.write_topic(args.page, data, args.id)

    def _is_blank_response(self, data: dict):
        if not self._extract_latest_url(data) or not self._extract_topics(data):
            return True

        return False

    def _get_data(self, target: TargetsEnum, args) -> Union[dict, None]:
        if target == TargetsEnum.TOPIC: # TODO replace with args isinstance() check
            url = TOPIC_URL.format(id=args.id)
            rate_limiter = self.cons_limiter
        elif target == TargetsEnum.LATEST:
            url = args.url  # take as is in response
            rate_limiter = self.prods_limiter

        try:
            logger.msg(EventsEnum.data_get.value)
            with rate_limiter.ratelimit('default-idnt', delay=True):
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


def run_latest(host,
               consumers_n=RunDefaults.consumers_n,
               producers_n=RunDefaults.producers_n,
               start_page=RunDefaults.start_page,
               prod_rates=RunDefaults.prod_rates,
               cons_rates=RunDefaults.cons_rates):
               
    logger.msg(EventsEnum.started.value)

    # Initialization
    start_iso_date = datetime.now(timezone.utc).isoformat(timespec='seconds')
    method = 'latest'
    timeout = Timeout(15.0)
    client = Client(base_url='https://' + host, headers={'accept': 'application/json'},
                    limits=Limits(max_connections=10,
                                  max_keepalive_connections=10,
                                  keepalive_expiry=5),
                    timeout=timeout)
    stop_event = StopEvent()
    params = CmdLatestParams(page=start_page)
    target_dir = TargetDir(method, params, start_iso_date)
    task_exchanger = TaskExchanger()
    prods_limiter = Limiter(*prod_rates)
    cons_limiter = Limiter(*cons_rates)
    parser = Parser(client, target_dir, stop_event, task_exchanger, prods_limiter, cons_limiter)

    # Create and enqueue a start url
    query_params = asdict(params)
    if params.page == 0:  # First page without param
        query_params.pop('page')
    query_params = urlencode(query_params)
    args = ParserLatestArgs(url=LATEST_URL + '?' + query_params)
    task_exchanger.put_latest(args)

    # Run pools
    prod_pool = [Thread(target=parser.parse_target,
                        args=(TargetsEnum.LATEST, ),
                        name=f'Producer-{i}',
                        daemon=True)
                 for i in range(1, producers_n + 1) ] 
    cons_pool = [Thread(target=parser.parse_target,
                        args=(TargetsEnum.TOPIC, ),
                        name=f'Consumer-{i}',
                        daemon=True)
                 for i in range(1, consumers_n + 1) ] 

    # Let the consumers get started first
    for thread in chain(cons_pool, prod_pool):
        thread.start()

    # logger.msg('stop.wait')
    # stop_event.wait()
        
    # Normal termination
    logger.msg('threads.join')
    for thread in chain(cons_pool, prod_pool):
        thread.join()

    logger.msg(EventsEnum.finished.value)

