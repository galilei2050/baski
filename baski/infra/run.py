"""Cloud Run service provisioning helpers (secrets, repo SHA tagging, monitoring alerts)."""

from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

import git
import pulumi_gcp as gcp

__all__ = [
    "CloudRunService",
    "CloudRunServiceConfig",
    "create_cloud_run_env",
    "create_cloud_run_secret_env",
    "create_cloud_run_with_monitoring",
    "repo_sha",
    "repo_short_sha",
]


class CloudRunService(NamedTuple):
    """Resources created by :func:`create_cloud_run_with_monitoring`."""

    service: gcp.cloudrunv2.Service
    alert_policy: gcp.monitoring.AlertPolicy
    iam_member: gcp.cloudrun.IamMember | None


@dataclass(frozen=True)
class CloudRunServiceConfig:
    """Bundle of options for :func:`create_cloud_run_with_monitoring`."""

    service_name: str
    image: str
    envs: list
    resources: gcp.cloudrunv2.ServiceTemplateContainerResourcesArgs
    service_account_email: str
    notification_channels: list
    args: list | None = None
    allow_unauthenticated: bool = False
    location: str | None = None
    ingress: str = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"
    min_instances: int = 0
    max_instances: int = 8


def create_cloud_run_env(name: str, value: str) -> gcp.cloudrunv2.ServiceTemplateContainerEnvArgs:
    """Return a Cloud Run env-var args object with a plain literal value."""
    return gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(name=name, value=value)


def create_cloud_run_secret_env(secret_id: str, service_name: str) -> gcp.cloudrunv2.ServiceTemplateContainerEnvArgs:
    """Return a Cloud Run env-var args object that resolves to the latest version of a Secret Manager secret."""
    secret = gcp.secretmanager.Secret.get(f"{service_name}_secret_{secret_id}", secret_id)
    return gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
        name=secret_id,
        value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
            secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                secret=secret.secret_id,
                version="latest",
            ),
        ),
    )


def repo_sha(repo_path: str | Path = ".") -> str:
    """Return the full HEAD commit SHA of the git repo containing ``repo_path``."""
    return git.Repo(str(repo_path), search_parent_directories=True).head.commit.hexsha


def repo_short_sha(repo_path: str | Path = ".") -> str:
    """Return the 7-character short HEAD commit SHA of the git repo containing ``repo_path``."""
    return repo_sha(repo_path)[:7]


def _create_5xx_alert_policy(service_name: str, notification_channels: list) -> gcp.monitoring.AlertPolicy:
    return gcp.monitoring.AlertPolicy(
        f"{service_name}-5xx-alert",
        display_name=f"5xx Errors: {service_name}",
        documentation=gcp.monitoring.AlertPolicyDocumentationArgs(
            content=(
                f"5xx errors detected in Cloud Run service {service_name}. "
                f"Check application logs and investigate user impact."
            ),
            mime_type="text/markdown",
        ),
        conditions=[
            gcp.monitoring.AlertPolicyConditionArgs(
                display_name="5xx error count > 0",
                condition_threshold=gcp.monitoring.AlertPolicyConditionConditionThresholdArgs(
                    filter=(
                        f'resource.type="cloud_run_revision" '
                        f'AND resource.labels.service_name="{service_name}" '
                        f'AND metric.type="run.googleapis.com/request_count" '
                        f'AND metric.labels.response_code_class="5xx"'
                    ),
                    comparison="COMPARISON_GT",
                    threshold_value=0,
                    duration="60s",
                    aggregations=[
                        gcp.monitoring.AlertPolicyConditionConditionThresholdAggregationArgs(
                            alignment_period="60s",
                            per_series_aligner="ALIGN_RATE",
                            cross_series_reducer="REDUCE_SUM",
                        )
                    ],
                ),
            )
        ],
        combiner="OR",
        enabled=True,
        notification_channels=notification_channels,
        project=gcp.config.project,
    )


def create_cloud_run_with_monitoring(config: CloudRunServiceConfig) -> CloudRunService:
    """Create a Cloud Run v2 service with a 5xx alert policy and optional public-invoke IAM binding."""
    location = config.location or gcp.config.region
    envs = [
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(name="GOOGLE_CLOUD_PROJECT", value=gcp.config.project),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(name="GOOGLE_CLOUD_REGION", value=location),
        *config.envs,
    ]
    container_args = gcp.cloudrunv2.ServiceTemplateContainerArgs(
        image=config.image,
        args=config.args,
        envs=envs,
        resources=config.resources,
    )
    service = gcp.cloudrunv2.Service(
        f"{config.service_name}-cloud-run-service",
        name=config.service_name,
        location=location,
        ingress=config.ingress,
        template=gcp.cloudrunv2.ServiceTemplateArgs(
            scaling=gcp.cloudrunv2.ServiceTemplateScalingArgs(
                min_instance_count=config.min_instances,
                max_instance_count=config.max_instances,
            ),
            service_account=config.service_account_email,
            containers=[container_args],
        ),
    )
    iam_member = None
    if config.allow_unauthenticated:
        iam_member = gcp.cloudrun.IamMember(
            f"allow-unauthenticated-{config.service_name}-cloud-run-service",
            service=service.name,
            location=service.location,
            role="roles/run.invoker",
            member="allUsers",
        )
    error_alert_policy = _create_5xx_alert_policy(config.service_name, config.notification_channels)
    return CloudRunService(service, error_alert_policy, iam_member)
