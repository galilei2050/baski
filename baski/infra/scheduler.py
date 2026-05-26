import base64
import json

import pulumi_gcp as gcp

__all__ = ["make_scheduled_job"]


def make_scheduled_job(
    name: str,
    description: str,
    schedule: str,
    uri: str,
    service_account: gcp.serviceaccount.Account,
    time_zone: str = "America/Los_Angeles",
    body: dict | None = None,
):
    http_target = {
        "httpMethod": "POST",
        "uri": uri,
        "headers": {"Content-Type": "application/json"},
        "oidc_token": {
            "service_account_email": service_account.email,
            "audience": uri,
        },
    }
    if isinstance(body, dict):
        json_string = json.dumps(body).strip()
        http_target["body"] = base64.b64encode(json_string.encode("utf-8")).decode("utf-8")

    return gcp.cloudscheduler.Job(
        f"cloud-scheduler-job-{name}",
        name=name,
        description=description,
        schedule=schedule,
        time_zone=time_zone,
        http_target=http_target,
        attempt_deadline="1800s",
    )
