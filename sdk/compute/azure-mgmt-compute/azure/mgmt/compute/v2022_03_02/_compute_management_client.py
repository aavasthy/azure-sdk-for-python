# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# Code generated by Microsoft (R) AutoRest Code Generator.
# Changes may cause incorrect behavior and will be lost if the code is regenerated.
# --------------------------------------------------------------------------

from copy import deepcopy
from typing import Any, Optional, TYPE_CHECKING, cast
from typing_extensions import Self

from azure.core.pipeline import policies
from azure.core.rest import HttpRequest, HttpResponse
from azure.core.settings import settings
from azure.mgmt.core import ARMPipelineClient
from azure.mgmt.core.policies import ARMAutoResourceProviderRegistrationPolicy
from azure.mgmt.core.tools import get_arm_endpoints

from . import models as _models
from ._configuration import ComputeManagementClientConfiguration
from ._utils.serialization import Deserializer, Serializer
from .operations import (
    DiskAccessesOperations,
    DiskEncryptionSetsOperations,
    DiskRestorePointOperations,
    DisksOperations,
    SnapshotsOperations,
)

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential


class ComputeManagementClient:
    """Compute Client.

    :ivar disks: DisksOperations operations
    :vartype disks: azure.mgmt.compute.v2022_03_02.operations.DisksOperations
    :ivar disk_accesses: DiskAccessesOperations operations
    :vartype disk_accesses: azure.mgmt.compute.v2022_03_02.operations.DiskAccessesOperations
    :ivar disk_encryption_sets: DiskEncryptionSetsOperations operations
    :vartype disk_encryption_sets:
     azure.mgmt.compute.v2022_03_02.operations.DiskEncryptionSetsOperations
    :ivar disk_restore_point: DiskRestorePointOperations operations
    :vartype disk_restore_point:
     azure.mgmt.compute.v2022_03_02.operations.DiskRestorePointOperations
    :ivar snapshots: SnapshotsOperations operations
    :vartype snapshots: azure.mgmt.compute.v2022_03_02.operations.SnapshotsOperations
    :param credential: Credential needed for the client to connect to Azure. Required.
    :type credential: ~azure.core.credentials.TokenCredential
    :param subscription_id: Subscription credentials which uniquely identify Microsoft Azure
     subscription. The subscription ID forms part of the URI for every service call. Required.
    :type subscription_id: str
    :param base_url: Service URL. Default value is None.
    :type base_url: str
    :keyword api_version: Api Version. Default value is "2022-03-02". Note that overriding this
     default value may result in unsupported behavior.
    :paramtype api_version: str
    :keyword int polling_interval: Default waiting time between two polls for LRO operations if no
     Retry-After header is present.
    """

    def __init__(
        self, credential: "TokenCredential", subscription_id: str, base_url: Optional[str] = None, **kwargs: Any
    ) -> None:
        _cloud = kwargs.pop("cloud_setting", None) or settings.current.azure_cloud  # type: ignore
        _endpoints = get_arm_endpoints(_cloud)
        if not base_url:
            base_url = _endpoints["resource_manager"]
        credential_scopes = kwargs.pop("credential_scopes", _endpoints["credential_scopes"])
        self._config = ComputeManagementClientConfiguration(
            credential=credential, subscription_id=subscription_id, credential_scopes=credential_scopes, **kwargs
        )

        _policies = kwargs.pop("policies", None)
        if _policies is None:
            _policies = [
                policies.RequestIdPolicy(**kwargs),
                self._config.headers_policy,
                self._config.user_agent_policy,
                self._config.proxy_policy,
                policies.ContentDecodePolicy(**kwargs),
                ARMAutoResourceProviderRegistrationPolicy(),
                self._config.redirect_policy,
                self._config.retry_policy,
                self._config.authentication_policy,
                self._config.custom_hook_policy,
                self._config.logging_policy,
                policies.DistributedTracingPolicy(**kwargs),
                policies.SensitiveHeaderCleanupPolicy(**kwargs) if self._config.redirect_policy else None,
                self._config.http_logging_policy,
            ]
        self._client: ARMPipelineClient = ARMPipelineClient(base_url=cast(str, base_url), policies=_policies, **kwargs)

        client_models = {k: v for k, v in _models.__dict__.items() if isinstance(v, type)}
        self._serialize = Serializer(client_models)
        self._deserialize = Deserializer(client_models)
        self._serialize.client_side_validation = False
        self.disks = DisksOperations(self._client, self._config, self._serialize, self._deserialize, "2022-03-02")
        self.disk_accesses = DiskAccessesOperations(
            self._client, self._config, self._serialize, self._deserialize, "2022-03-02"
        )
        self.disk_encryption_sets = DiskEncryptionSetsOperations(
            self._client, self._config, self._serialize, self._deserialize, "2022-03-02"
        )
        self.disk_restore_point = DiskRestorePointOperations(
            self._client, self._config, self._serialize, self._deserialize, "2022-03-02"
        )
        self.snapshots = SnapshotsOperations(
            self._client, self._config, self._serialize, self._deserialize, "2022-03-02"
        )

    def _send_request(self, request: HttpRequest, *, stream: bool = False, **kwargs: Any) -> HttpResponse:
        """Runs the network request through the client's chained policies.

        >>> from azure.core.rest import HttpRequest
        >>> request = HttpRequest("GET", "https://www.example.org/")
        <HttpRequest [GET], url: 'https://www.example.org/'>
        >>> response = client._send_request(request)
        <HttpResponse: 200 OK>

        For more information on this code flow, see https://aka.ms/azsdk/dpcodegen/python/send_request

        :param request: The network request you want to make. Required.
        :type request: ~azure.core.rest.HttpRequest
        :keyword bool stream: Whether the response payload will be streamed. Defaults to False.
        :return: The response of your network call. Does not do error handling on your response.
        :rtype: ~azure.core.rest.HttpResponse
        """

        request_copy = deepcopy(request)
        request_copy.url = self._client.format_url(request_copy.url)
        return self._client.send_request(request_copy, stream=stream, **kwargs)  # type: ignore

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> Self:
        self._client.__enter__()
        return self

    def __exit__(self, *exc_details: Any) -> None:
        self._client.__exit__(*exc_details)
