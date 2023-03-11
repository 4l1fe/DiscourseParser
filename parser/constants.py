from enum import Enum

from pyrate_limiter import RequestRate, Duration


FORUM_HOST = 'https://discuss.streamlit.io'
LATEST_URL = '/latest'
TOPIC_URL = '/t/{id}'
TASK_CHECK_TIMEOUT = 2  # seconds
REQUEST_DELAY = 0.300  # ms


class EventsEnum(Enum):
    error_connection = 'error.connection'
    error_http = 'error.http'
    error_file = 'error.file'
    data_received = 'data.received'
    data_saved = 'data.saved'
    data_get = 'data.get'
    started = 'started'
    do = 'do'
    done = 'done'
    finished = 'finished'
    stop_signal = 'stop.signal'
    stop_blank = 'stop.blank'
    task_get = 'task.get'


class TargetsEnum(Enum):
    TOPIC = 'topic'
    LATEST = 'latest'


class RunDefaults:
    consumers_n = 1
    producers_n = 1
    start_page = 0
    prod_rates = [RequestRate(0.2, Duration.SECOND),
                  RequestRate(6, Duration.MINUTE)],
    cons_rates = [RequestRate(3, Duration.SECOND), ]
