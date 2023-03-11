from dataclasses import dataclass

from constants import RunDefaults


@dataclass(order=True)  # dirs generation order
class CmdLatestParams:
    order: str = 'created'
    ascending: bool = False
    page: int = RunDefaults.start_page


@dataclass
class ParserLatestArgs:
    url: str = ''


@dataclass
class ParserTopicArgs:
    page: int
    id: int

