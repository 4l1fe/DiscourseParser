from enum import Enum


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
