import pulumi_gcp as gcp

__all__ = ["make_cloud_run_backend"]


def make_cloud_run_backend(
    name: str,
    service: gcp.cloudrunv2.Service,
    compression: bool = False,
) -> tuple[gcp.compute.RegionNetworkEndpointGroup, gcp.compute.BackendService]:
    cloud_run_neg = gcp.compute.RegionNetworkEndpointGroup(
        f"compute-neg-for-cloud-run-{name}",
        network_endpoint_type="SERVERLESS",
        region=gcp.config.region,
        cloud_run=gcp.compute.RegionNetworkEndpointGroupCloudRunArgs(service=service.name),
    )
    cdn_policy = gcp.compute.BackendServiceCdnPolicyArgs(
        cache_mode="CACHE_ALL_STATIC",
        cache_key_policy=gcp.compute.BackendServiceCdnPolicyCacheKeyPolicyArgs(
            include_host=True,
            include_protocol=True,
            include_query_string=True,
        ),
    )
    backend_service = gcp.compute.BackendService(
        f"compute-backend-service-for-cloud-run-{name}",
        protocol="HTTPS",
        port_name="http",
        backends=[gcp.compute.BackendServiceBackendArgs(group=cloud_run_neg.self_link)],
        compression_mode="AUTOMATIC" if compression else None,
        enable_cdn=compression,
        load_balancing_scheme="EXTERNAL_MANAGED",
        cdn_policy=cdn_policy if compression else None,
    )
    return cloud_run_neg, backend_service
