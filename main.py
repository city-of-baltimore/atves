""" atves main script """
import argparse
from datetime import date, datetime, timedelta

from atves.atves_database import AtvesDatabase
from atves.conduent import OVERHEIGHT, REDLIGHT

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
parser.add_argument('-b', '--builddb', action='store_true', help="Rebuilds (or updates) the camera location database")

args = parser.parse_args()
ad = AtvesDatabase()

start_date = datetime(args.year, args.month, args.day)
end_date = (datetime(args.year, args.month, args.day, 23, 59, 59) + timedelta(days=args.numofdays - 1))

all_cams = bool(args.allcams or not any([args.oh, args.rl, args.tc]))

# Build the camera database
if args.builddb:
    ad.build_location_db()

# Process traffic cameras
if args.tc or all_cams:
    ad.process_traffic_count_data(start_date, end_date)

# Process over height cameras
if args.oh or all_cams:
    ad.process_conduent_reject_numbers(start_date, end_date, OVERHEIGHT)
    ad.process_conduent_data_by_location(start_date, end_date, OVERHEIGHT)
    ad.process_conduent_data_approval_by_review_date(start_date, end_date, OVERHEIGHT)

# Process red light cameras
if args.rl or all_cams:
    ad.process_conduent_reject_numbers(start_date, end_date, REDLIGHT)
    ad.process_conduent_data_by_location(start_date, end_date, REDLIGHT)
    ad.process_conduent_data_amber_time(start_date, end_date)
    ad.process_conduent_data_approval_by_review_date(start_date, end_date, REDLIGHT)
