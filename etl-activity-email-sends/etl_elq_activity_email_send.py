"""
    Beam/Dataflow pipeline to export Eloqua email send activities and load
    in to BigQuery
"""

import apache_beam as beam
from pyeloqua import Bulk

# Constants

PROJECT = 'gcp-project-name'
DISPOSITION = beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND
ELOQUA_COMPANY = 'company'
ELOQUA_USER = 'user'
ELOQUA_PASSWORD = 'password'

# Functions

def export_elq_activity(activity, start, end):
    """
    Export all activities in start/end date

    :param string activity: Eloqua activity name; see pyeloqua documentation
    :param datetime start: beginning of export range
    :param string start: beginning of export range; format %Y-%m-%d %H:%M:%S
    :param datetime end: end of export range
    :param string end: end of export range; format %Y-%m-%d %H:%M:%S
    :return dict: returns dict (JSON) of exported activity data
    """

    elq = Bulk(
        company=ELOQUA_COMPANY,
        username=ELOQUA_USER,
        password=ELOQUA_PASSWORD
    )

    # specify export and activity type
    elq.exports('activities', act_type=activity)

    # Specify all activity type fields
    # Can be limited to desired fields; see reference docs:
    # https://github.com/colemanja91/pyeloqua/blob/master/pyeloqua/system_fields.py
    elq.add_fields()

    elq.filter_date('ActivityDate', start=start, end=end)

    elq.add_options(areSystemTimestampsInUTC=True)

    elq.create_def('dataflow_{activity}_{start}_{end}'.format(
        activity=activity,
        start=start,
        end=end
    ))

    elq.handle_sync()

    activities = elq.get_export_data()

    return activities


# Pipeline

def run():
    """ Define and run Apache Beam pipeline """

    argv = [
        '--project={0}'.format(PROJECT),
        '--job_name=etleloquaactivityemailsend',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(PROJECT),
        '--temp_location=gs://{0}/staging/'.format(PROJECT),
        '--runner=DataflowRunner',
        '--requirements_file=./requirements.txt'
    ]

    pipeline = beam.Pipeline(argv=argv)

    activities = pipeline | 'GetEloquaActivityEmailSend' >> beam.Create(export_elq_activity(
        'EmailSend', '2017-10-01 00:00:00', '2017-10-02 00:00:00'
        ))

    write = activities | 'WriteToBQ' >> beam.io.WriteToBigQuery(
        table='activity_email_send',
        dataset='eloqua',
        project=PROJECT,
        write_disposition=DISPOSITION
    )

    pipeline.run()


if __name__ == '__main__':
    run()
