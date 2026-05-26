from pathlib import Path

import git
import pulumi_gcp as gcp

__all__ = [
    "create_cloud_run_secret_env",
    "create_cloud_run_with_monitoring",
    "repo_sha",
    "repo_short_sha",
]


def create_cloud_run_secret_env(secret_id: str, service_name: str) -> gcp.cloudrunv2.ServiceTemplateContainerEnvArgs:
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
    return git.Repo(str(repo_path), search_parent_directories=True).head.commit.hexsha


def repo_short_sha(repo_path: str | Path = ".") -> str:
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


def create_cloud_run_with_monitoring(  # noqa: PLR0913 — Cloud Run wrappers legitimately take many parameters
    service_name: str,
    image: str,
    envs: list,
    resources: gcp.cloudrunv2.ServiceTemplateContainerResourcesArgs,
    service_account_email: str,
    notification_channels: list,
    args: list | None = None,
    allow_unauthenticated: bool = False,
    location: str | None = None,
    ingress: str = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER",
    min_instances: int = 0,
    max_instances: int = 8,
) -> tuple[gcp.cloudrunv2.Service, gcp.monitoring.AlertPolicy, gcp.cloudrun.IamMember | None]:
    envs = [
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(name="GOOGLE_CLOUD_PROJECT", value=gcp.config.project),
        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
            name="GOOGLE_CLOUD_REGION",
            value=location or gcp.config.region,
        ),
        *envs,
    ]
    container_args = gcp.cloudrunv2.ServiceTemplateContainerArgs(
        image=image,
        args=args,
        envs=envs,
        resources=resources,
    )
    service = gcp.cloudrunv2.Service(
        f"{service_name}-cloud-run-service",
        name=service_name,
        location=location or gcp.config.region,
        ingress=ingress,
        template=gcp.cloudrunv2.ServiceTemplateArgs(
            scaling=gcp.cloudrunv2.ServiceTemplateScalingArgs(
                min_instance_count=min_instances,
                max_instance_count=max_instances,
            ),
            service_account=service_account_email,
            containers=[container_args],
        ),
    )
    iam_member = None
    if allow_unauthenticated:
        iam_member = gcp.cloudrun.IamMember(
            f"allow-unauthenticated-{service_name}-cloud-run-service",
            service=service.name,
            location=service.location,
            role="roles/run.invoker",
            member="allUsers",
        )
    error_alert_policy = _create_5xx_alert_policy(service_name, notification_channels)
    return service, error_alert_policy, iam_member
