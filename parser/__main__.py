from argparse import ArgumentParser

from parser import run_latest


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
