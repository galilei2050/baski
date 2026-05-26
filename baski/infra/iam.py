"""IAM helpers for referencing existing GCP service accounts in Pulumi stacks."""

import pulumi_gcp as gcp

__all__ = ["get_service_account"]


def get_service_account(account_id: str, domain: str | None = None) -> gcp.serviceaccount.Account:
    """Look up an existing GCP service account by ID (defaults domain to the current project)."""
    domain = domain or f"{gcp.config.project}.iam.gserviceaccount.com"
    return gcp.serviceaccount.Account.get(
        id=f"projects/{gcp.config.project}/serviceAccounts/{account_id}@{domain}",
        resource_name=f"{account_id}-service-account",
        account_id=f"{account_id}@{domain}",
    )
