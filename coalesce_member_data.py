from collections import defaultdict
from logging import getLogger

from requests import get
from requests.exceptions import Timeout

_APIS_TO_CHECK = [
    "https://api1.com?member_id={}",
    "https://api2.com?member_id={}",
    "https://api3.com?member_id={}",
]


def average(
    member_data_source,
):
    """
    Default coalescer for coalesce_member_data, assumes member_data outputs are all numeric.
    """
    if not member_data_source:
        raise ValueError("Invalid member data source")

    count = 0
    field_tracker = defaultdict(int)

    for member_data in member_data_source:
        count += 1
        for field in member_data:
            field_tracker[field] += member_data[field]

    for field in field_tracker:
        field_tracker[field] /= count

    return field_tracker


def coalesce_member_data(member_id, coalescer=average):
    """
    For the given member ID, run the coalescer on the URLs for member data with it as the argument.

    """
    if member_id is None:
        raise ValueError("Invalid member ID")

    logger = getLogger(__name__)

    def _create_api_generator(member_id):
        for api_url in _APIS_TO_CHECK:
            try:
                target = api_url.format(member_id)
                # Timeout is represented in seconds, so this timesout after a minute.
                response = get(target, timeout=60)
            except Timeout:
                logger.error(
                    f'Timeout when attempting to read data for member ID "{member_id}" from {target}'
                )
                continue
            yield response.json()

    return coalescer(_create_api_generator(member_id))