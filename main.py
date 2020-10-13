import argparse
from datetime import date, timedelta

from atves.atves_util import build_location_db, process_axsis_data, process_citeweb_data
from atves.citeweb import ALLCAMS, OVERHEIGHT, REDLIGHT

yesterday = date.today() - timedelta(days=1)
parser = argparse.ArgumentParser(description='Traffic count importer')
parser.add_argument('-m', '--month', type=int, default=yesterday.month,
                    help=('Optional: Month of date we should start searching on (IE: 10 for Oct). Defaults to '
                          'yesterday if not specified'))
parser.add_argument('-d', '--day', type=int, default=yesterday.day,
                    help=('Optional: Day of date we should start searching on (IE: 5). Defaults to yesterday if '
                          'not specified'))
parser.add_argument('-y', '--year', type=int, default=yesterday.year,
                    help=('Optional: Four digit year we should start searching on (IE: 2020). Defaults to '
                          'yesterday if not specified'))
parser.add_argument('-n', '--numofdays', default=1, type=int,
                    help='Optional: Number of days to search, including the start date.')
parser.add_argument('-a', '--allcams', action='store_true', help="Process all camera types")
parser.add_argument('-o', '--oh', action='store_true', help="Process only over height cameras")
parser.add_argument('-r', '--rl', action='store_true', help="Process only red light cameras")
parser.add_argument('-t', '--tc', action='store_true', help="Process only traffic counts")

args = parser.parse_args()
build_location_db()

all_cams = bool(args.allcams or not any([args.oh, args.rl, args.tc]))

if args.tc or all_cams:
    # Process traffic counts from speed cameras
    process_axsis_data(args.year, args.month, args.day, args.numofdays)

# Process red light and over height cameras
if all_cams or (args.oh and args.rl):
    process_citeweb_data(args.year, args.month, args.day, args.numofdays, ALLCAMS)
elif args.oh:
    process_citeweb_data(args.year, args.month, args.day, args.numofdays, OVERHEIGHT)
elif args.rl:
    process_citeweb_data(args.year, args.month, args.day, args.numofdays, REDLIGHT)
