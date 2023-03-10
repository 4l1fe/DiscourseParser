from dataclasses import dataclass


@dataclass(order=True)  # dirs generation order
class CmdLatestParams:
    order: str = 'created'
    ascending: bool = False
    page: int = 0


@dataclass
class ParserLatestArgs:
    url: str = ''


@dataclass
class ParserTopicArgs:
    page: int
    id: int

