"""
    Cookie-cutter template for exporting Eloqua activities
"""

from pyeloqua import Bulk

# Constants

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

    elq.create_def('dataflow_{activity}_{start}_{end}'.format(
        activity=activity,
        start=start,
        end=end
    ))

    elq.handle_sync()
    
    activities = elq.get_export_data()

    return activities
