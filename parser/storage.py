import json
from pathlib import Path
from threading import RLock
from dataclasses import asdict

import structlog

from containers import CmdLatestParams
from constants import EventsEnum, TargetsEnum


logger = structlog.get_logger()


class TargetDir:
    
    def __init__(self, method: str, params: CmdLatestParams, start_iso_date: str):
        self.start_iso_date = start_iso_date
        self.target_dir = self._create_target_dir(method, params)
        self.rlock = RLock()

    def write_latest(self, page, data):
        self._write_data(TargetsEnum.LATEST, page, data)

    def write_topic(self, page, data, topic_id):
        self._write_data(TargetsEnum.TOPIC, page, data, topic_id)

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
    
    def _write_data(self, target: TargetsEnum, page, data, *args):
        with self.rlock:
            self._set_page(page)
            text = json.dumps(data)
            get_path = self.get_topic_path
            if target == TargetsEnum.LATEST:
                get_path = self.get_latest_path
            
            p = get_path(*args)
            logr = logger.bind(path=p.as_posix())

            try:
                p.write_text(text)        
                logr.msg(EventsEnum.data_saved.value)
            except Exception as e:
                logr.msg(EventsEnum.error_file.value, exc_info=e)
