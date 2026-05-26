import pulumi
import pulumi_gcp as gcp

__all__ = ["DEFAULT_SUBSCRIPTION_KWARGS", "create_subscription_with_push_and_dlq", "make_topic"]

is_pulumi = pulumi.runtime.is_dry_run() is not None


DEFAULT_SUBSCRIPTION_KWARGS = dict(
    project=gcp.config.project,
    message_retention_duration="604800s",  # 7 days
    retain_acked_messages=True,
    ack_deadline_seconds=600,  # 10 minutes
    expiration_policy={"ttl": ""},  # never
)


def make_topic(topic_name: str):
    if not is_pulumi:
        return None, None
    topic = gcp.pubsub.Topic(
        topic_name,
        name=topic_name,
        project=gcp.config.project,
    )
    debug_subscription = gcp.pubsub.Subscription(
        f"{topic_name}-debug",
        name=f"{topic_name}-debug",
        topic=topic.name,
        **DEFAULT_SUBSCRIPTION_KWARGS,
    )
    return topic, debug_subscription


def _create_dlq_topic(topic_name: str, subscription_name: str):
    dlq_topic_name = f"{topic_name}-{subscription_name}-dlq"
    dlq_topic = gcp.pubsub.Topic(
        dlq_topic_name,
        name=dlq_topic_name,
        project=gcp.config.project,
    )
    dlq_debug_subscription = gcp.pubsub.Subscription(
        f"{dlq_topic_name}-debug",
        name=f"{dlq_topic_name}-debug",
        topic=dlq_topic.name,
        **DEFAULT_SUBSCRIPTION_KWARGS,
    )
    return dlq_topic, dlq_debug_subscription, dlq_topic_name


def _create_push_subscription(
    topic_name: str,
    subscription_name: str,
    http_endpoint: str,
    service_account: gcp.serviceaccount.Account,
    dlq_topic_name: str,
    backoff_max_seconds: int | None = None,
):
    max_backoff = f"{backoff_max_seconds}s" if backoff_max_seconds is not None else "600s"
    return gcp.pubsub.Subscription(
        f"{topic_name}-{subscription_name}",
        name=f"{topic_name}-{subscription_name}",
        topic=topic_name,
        push_config=gcp.pubsub.SubscriptionPushConfigArgs(
            push_endpoint=http_endpoint,
            oidc_token=gcp.pubsub.SubscriptionPushConfigOidcTokenArgs(
                service_account_email=service_account.email,
            ),
            no_wrapper=gcp.pubsub.SubscriptionPushConfigNoWrapperArgs(write_metadata=True),
        ),
        retry_policy=gcp.pubsub.SubscriptionRetryPolicyArgs(
            minimum_backoff="10s",
            maximum_backoff=max_backoff,
        ),
        dead_letter_policy=gcp.pubsub.SubscriptionDeadLetterPolicyArgs(
            max_delivery_attempts=5,
            dead_letter_topic=f"projects/{gcp.config.project}/topics/{dlq_topic_name}",
        ),
        **DEFAULT_SUBSCRIPTION_KWARGS,
    )


def _create_dlq_alert_policy(
    topic_name: str,
    subscription_name: str,
    dlq_topic_name: str,
    notification_channels: list,
):
    return gcp.monitoring.AlertPolicy(
        f"{dlq_topic_name}-alert",
        display_name=f"DLQ Messages: {topic_name}-{subscription_name}",
        documentation=gcp.monitoring.AlertPolicyDocumentationArgs(
            content=(
                f"Messages are accumulating in DLQ for {topic_name}-{subscription_name}. "
                f"Check integration health and debug subscription for failed messages."
            ),
            mime_type="text/markdown",
        ),
        conditions=[
            gcp.monitoring.AlertPolicyConditionArgs(
                display_name="DLQ message count > 0",
                condition_threshold=gcp.monitoring.AlertPolicyConditionConditionThresholdArgs(
                    filter=(
                        f'resource.type="pubsub_subscription" '
                        f'AND resource.labels.subscription_id="{dlq_topic_name}-debug" '
                        f'AND metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages"'
                    ),
                    comparison="COMPARISON_GT",
                    threshold_value=0,
                    duration="300s",
                    aggregations=[
                        gcp.monitoring.AlertPolicyConditionConditionThresholdAggregationArgs(
                            alignment_period="300s",
                            per_series_aligner="ALIGN_MAX",
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


def create_subscription_with_push_and_dlq(
    topic_name: str,
    subscription_name: str,
    http_endpoint: str,
    service_account: gcp.serviceaccount.Account,
    notification_channels: list,
    backoff_max_seconds: int | None = None,
):
    if not is_pulumi:
        return None, None, None, None

    dlq_topic, dlq_debug_subscription, dlq_topic_name = _create_dlq_topic(topic_name, subscription_name)
    subscription = _create_push_subscription(
        topic_name=topic_name,
        subscription_name=subscription_name,
        http_endpoint=http_endpoint,
        service_account=service_account,
        dlq_topic_name=dlq_topic_name,
        backoff_max_seconds=backoff_max_seconds,
    )
    alert_policy = _create_dlq_alert_policy(
        topic_name=topic_name,
        subscription_name=subscription_name,
        dlq_topic_name=dlq_topic_name,
        notification_channels=notification_channels,
    )
    return dlq_topic, dlq_debug_subscription, subscription, alert_policy
