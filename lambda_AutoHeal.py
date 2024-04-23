#!/usr/bin/python3.6
import urllib3
import json

http = urllib3.PoolManager()


def lambda_handler(event, context):
    url = "https://hooks.slack.com/services/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"
    msg = {
        "channel": "#tpt_alteryx_queue",
        "username": "Alteryx_Service",
        "text": event["Records"][0]["Sns"]["Message"],
        "icon_emoji": "",
    }

    encoded_msg = json.dumps(msg).encode("utf-8")
    resp = http.request("POST", url, body=encoded_msg)
    print(
        {
            "message": event["Records"][0]["Sns"]["Message"],
            "status_code": resp.status,
            "response": resp.data,
        }
    )