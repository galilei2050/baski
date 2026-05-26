"""Cloud Scheduler job helpers for invoking HTTPS endpoints with OIDC auth."""

import base64
import json

import pulumi_gcp as gcp

__all__ = ["make_scheduled_job"]


def make_scheduled_job(  # noqa: PLR0913 — Cloud Scheduler jobs legitimately take many configuration parameters
    name: str,
    description: str,
    schedule: str,
    uri: str,
    service_account: gcp.serviceaccount.Account,
    time_zone: str = "America/Los_Angeles",
    body: dict | None = None,  # noqa: ANON002 — arbitrary JSON body for scheduled job target
) -> gcp.cloudscheduler.Job:
    """Create a Cloud Scheduler HTTPS job that POSTs ``body`` (JSON) with an OIDC token."""
    http_body: str | None = None
    if isinstance(body, dict):
        http_body = base64.b64encode(json.dumps(body).strip().encode("utf-8")).decode("utf-8")

    http_target = gcp.cloudscheduler.JobHttpTargetArgs(
        http_method="POST",
        uri=uri,
        headers={"Content-Type": "application/json"},
        oidc_token=gcp.cloudscheduler.JobHttpTargetOidcTokenArgs(
            service_account_email=service_account.email,
            audience=uri,
        ),
        body=http_body,
    )

    return gcp.cloudscheduler.Job(
        f"cloud-scheduler-job-{name}",
        name=name,
        description=description,
        schedule=schedule,
        time_zone=time_zone,
        http_target=http_target,
        attempt_deadline="1800s",
    )
