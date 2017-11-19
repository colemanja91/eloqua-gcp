"""
    Run a backfill operation
"""

from argparse import ArgumentParser

from etl_elq_bq import run

if __name__ == '__main__':
    PARSER = ArgumentParser()
    PARSER.add_argument('--activity',
                        dest='activity',
                        help='Eloqua activity type')

    PARSER.add_argument('--start',
                        dest='start',
                        help='Start date; format Y:m:dTH:M:S')

    PARSER.add_argument('--end',
                        dest='end',
                        help='End date; format Y:m:dTH:M:S')

    KNOWN_ARGS, OTHER_ARGS = PARSER.parse_known_args()

    run(
        activity=KNOWN_ARGS['activity'],
        start=KNOWN_ARGS['start'],
        end=KNOWN_ARGS['end']
    )
