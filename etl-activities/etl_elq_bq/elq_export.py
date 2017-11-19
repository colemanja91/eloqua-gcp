"""
    Eloqua activity export wrapper function
"""

from pyeloqua import Bulk

def export_elq_activity(elq_auth, activity, start, end):
    """
    Export all activities in start/end date

    :param dict elq_auth: dictionary containing Eloqua API auth credentials
    :param string activity: Eloqua activity name; see pyeloqua documentation
    :param string start: beginning of export range; format %Y-%m-%d %H:%M:%S
    :param string end: end of export range; format %Y-%m-%d %H:%M:%S
    :return dict: returns dict (JSON) of exported activity data
    """

    if activity == 'FormSubmit':
        raise Exception('FormSubmit activities currently unsupported due to RawData column')

    elq = Bulk(
        company=elq_auth['eloqua_company'],
        username=elq_auth['eloqua_username'],
        password=elq_auth['eloqua_password']
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
