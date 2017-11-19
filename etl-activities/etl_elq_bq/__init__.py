"""
    Init and helpers
"""

from argparse import ArgumentParser
from datetime import datetime, timedelta

from dataflow import run

# Functions
def backfill_run(activity, start, end):
    """
    Create multiple Dataflow pipelines to backfill activity data.
    Time ranges longer than one day will be split in to multiple 24-hour increments

    """

    def perdelta(start_date, end_date, delta):
        """ generate range of dates """

        output = []

        init = []

        curr = start_date
        init.append(curr)
        while curr < end_date:
            curr += delta
            init.append(curr)
            output.append(init)
            init = [curr]

        return output

    date_set = perdelta(
        datetime.strptime(start, '%H-%m-%dT%H:%M:%S'),
        datetime.strptime(end, '%H-%m-%dT%H:%M:%S'),
        delta=timedelta(hours=24)
    )

    for date_range in date_set:
        run(activity, date_range[0], date_range[1])


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

    PARSER.add_argument('--backfill',
                        dest='backfill',
                        default=True,
                        help='End date; format Y:m:dTH:M:S')

    KNOWN_ARGS, OTHER_ARGS = PARSER.parse_known_args()

    if KNOWN_ARGS['backfill'] is True:
        backfill_run(
            activity=KNOWN_ARGS['activity'],
            start=KNOWN_ARGS['start'],
            end=KNOWN_ARGS['end']
        )
    else:
        run(
            activity=KNOWN_ARGS['activity'],
            start=KNOWN_ARGS['start'],
            end=KNOWN_ARGS['end']
        )
