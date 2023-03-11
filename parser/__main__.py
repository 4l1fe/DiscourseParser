from argparse import ArgumentParser

from pyrate_limiter import RequestRate, Duration

from parser import run_latest
from constants import RunDefaults


def request_rates(string) -> list[RequestRate]:
    request_rates = []
    rate_items = string.split(';')
    for item in rate_items:
        limit, interval = item.split('/')
        rr = RequestRate(float(limit), float(interval))
        request_rates.append(rr)

    return request_rates


parser = ArgumentParser()
parser.add_argument('-cn', '--consumers', type=int, default=1)
parser.add_argument('-pn', '--producers', type=int, default=1)
parser.add_argument('-prr', '--prod-request-rates', type=request_rates, default=RunDefaults.prod_rates,
                    help="String of the following format: 3/1;4/60")
parser.add_argument('-crr', '--cons-request-rates', type=request_rates, default=RunDefaults.cons_rates)

subparsers = parser.add_subparsers(dest='command')

latest_parser = subparsers.add_parser('latest')
latest_parser.add_argument('host', help="Subdomain of a discourse community, example community.project.com.")
latest_parser.add_argument('-p', '--page', type=int, default=RunDefaults.start_page)


args = parser.parse_args()
if args.command == 'latest':
    run_latest(args.host,
               consumers_n=args.consumers,
               producers_n=args.producers,
               prod_rates=args.prod_request_rates,
               cons_rates=args.cons_request_rates,
               start_page=args.page)  
