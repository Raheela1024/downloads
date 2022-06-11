import requests
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync


def convert_to_snake_case(my_string):
    """
    This function will convert any string to snake_case string
    :param my_string: string to convert
    :return: string in camel_case format
    """
    return '_'.join(my_string.lower().split(" ")).replace("'", "")


def send_message(room_id, download_status=None, total_records=None, downloaded_records=None):
    print(room_id)
    print(download_status)
    print(total_records)
    print(downloaded_records)
    response = {
            'download_status': download_status,
            'total_records': total_records,
            'downloaded_records': downloaded_records

        }
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(f"submission_{room_id}", {
        "type": "submission_message",
        "response": response})
