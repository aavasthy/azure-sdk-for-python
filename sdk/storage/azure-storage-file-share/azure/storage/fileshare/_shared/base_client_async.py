# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
# mypy: disable-error-code="attr-defined"

import logging
from typing import Any, cast, Dict, Optional, Tuple, TYPE_CHECKING, Union

from azure.core.async_paging import AsyncList
from azure.core.credentials import AzureNamedKeyCredential, AzureSasCredential
from azure.core.credentials_async import AsyncTokenCredential
from azure.core.exceptions import HttpResponseError
from azure.core.pipeline import AsyncPipeline
from azure.core.pipeline.policies import (
    AsyncRedirectPolicy,
    AzureSasCredentialPolicy,
    ContentDecodePolicy,
    DistributedTracingPolicy,
    HttpLoggingPolicy,
)
from azure.core.pipeline.transport import AsyncHttpTransport

from .authentication import SharedKeyCredentialPolicy
from .base_client import create_configuration
from .constants import CONNECTION_TIMEOUT, DEFAULT_OAUTH_SCOPE, READ_TIMEOUT, SERVICE_HOST_BASE, STORAGE_OAUTH_SCOPE
from .models import StorageConfiguration
from .policies import (
    QueueMessagePolicy,
    StorageContentValidation,
    StorageHeadersPolicy,
    StorageHosts,
    StorageRequestHook,
)
from .policies_async import AsyncStorageBearerTokenCredentialPolicy, AsyncStorageResponseHook
from .response_handlers import PartialBatchErrorException, process_storage_error
from .._shared_access_signature import _is_credential_sastoken

if TYPE_CHECKING:
    from azure.core.pipeline.transport import HttpRequest, HttpResponse  # pylint: disable=C4756
_LOGGER = logging.getLogger(__name__)

_SERVICE_PARAMS = {
    "blob": {"primary": "BLOBENDPOINT", "secondary": "BLOBSECONDARYENDPOINT"},
    "queue": {"primary": "QUEUEENDPOINT", "secondary": "QUEUESECONDARYENDPOINT"},
    "file": {"primary": "FILEENDPOINT", "secondary": "FILESECONDARYENDPOINT"},
    "dfs": {"primary": "BLOBENDPOINT", "secondary": "BLOBENDPOINT"},
}


class AsyncStorageAccountHostsMixin(object):

    def _format_query_string(
        self,
        sas_token: Optional[str],
        credential: Optional[
            Union[str, Dict[str, str], "AzureNamedKeyCredential", "AzureSasCredential", AsyncTokenCredential]
        ],
        snapshot: Optional[str] = None,
        share_snapshot: Optional[str] = None,
    ) -> Tuple[
        str, Optional[Union[str, Dict[str, str], "AzureNamedKeyCredential", "AzureSasCredential", AsyncTokenCredential]]
    ]:
        query_str = "?"
        if snapshot:
            query_str += f"snapshot={snapshot}&"
        if share_snapshot:
            query_str += f"sharesnapshot={share_snapshot}&"
        if sas_token and isinstance(credential, AzureSasCredential):
            raise ValueError(
                "You cannot use AzureSasCredential when the resource URI also contains a Shared Access Signature."
            )
        if _is_credential_sastoken(credential):
            query_str += credential.lstrip("?")  # type: ignore [union-attr]
            credential = None
        elif sas_token:
            query_str += sas_token
        return query_str.rstrip("?&"), credential

    def _create_pipeline(
        self,
        credential: Optional[
            Union[str, Dict[str, str], AzureNamedKeyCredential, AzureSasCredential, AsyncTokenCredential]
        ] = None,
        **kwargs: Any,
    ) -> Tuple[StorageConfiguration, AsyncPipeline]:
        self._credential_policy: Optional[
            Union[AsyncStorageBearerTokenCredentialPolicy, SharedKeyCredentialPolicy, AzureSasCredentialPolicy]
        ] = None
        if hasattr(credential, "get_token"):
            if kwargs.get("audience"):
                audience = str(kwargs.pop("audience")).rstrip("/") + DEFAULT_OAUTH_SCOPE
            else:
                audience = STORAGE_OAUTH_SCOPE
            self._credential_policy = AsyncStorageBearerTokenCredentialPolicy(
                cast(AsyncTokenCredential, credential), audience
            )
        elif isinstance(credential, SharedKeyCredentialPolicy):
            self._credential_policy = credential
        elif isinstance(credential, AzureSasCredential):
            self._credential_policy = AzureSasCredentialPolicy(credential)
        elif credential is not None:
            raise TypeError(f"Unsupported credential: {type(credential)}")
        config = kwargs.get("_configuration") or create_configuration(**kwargs)
        if kwargs.get("_pipeline"):
            return config, kwargs["_pipeline"]
        transport = kwargs.get("transport")
        kwargs.setdefault("connection_timeout", CONNECTION_TIMEOUT)
        kwargs.setdefault("read_timeout", READ_TIMEOUT)
        if not transport:
            try:
                from azure.core.pipeline.transport import (  # pylint: disable=non-abstract-transport-import
                    AioHttpTransport,
                )
            except ImportError as exc:
                raise ImportError("Unable to create async transport. Please check aiohttp is installed.") from exc
            transport = AioHttpTransport(**kwargs)
        hosts = self._hosts
        policies = [
            QueueMessagePolicy(),
            config.proxy_policy,
            config.user_agent_policy,
            StorageContentValidation(),
            ContentDecodePolicy(response_encoding="utf-8"),
            AsyncRedirectPolicy(**kwargs),
            StorageHosts(hosts=hosts, **kwargs),
            config.retry_policy,
            config.headers_policy,
            StorageRequestHook(**kwargs),
            self._credential_policy,
            config.logging_policy,
            AsyncStorageResponseHook(**kwargs),
            DistributedTracingPolicy(**kwargs),
            HttpLoggingPolicy(**kwargs),
        ]
        if kwargs.get("_additional_pipeline_policies"):
            policies = policies + kwargs.get("_additional_pipeline_policies")  # type: ignore
        config.transport = transport  # type: ignore
        return config, AsyncPipeline(transport, policies=policies)  # type: ignore

    async def _batch_send(self, *reqs: "HttpRequest", **kwargs: Any) -> AsyncList["HttpResponse"]:
        """Given a series of request, do a Storage batch call.

        :param HttpRequest reqs: A collection of HttpRequest objects.
        :return: An AsyncList of HttpResponse objects.
        :rtype: AsyncList[HttpResponse]
        """
        # Pop it here, so requests doesn't feel bad about additional kwarg
        raise_on_any_failure = kwargs.pop("raise_on_any_failure", True)
        request = self._client._client.post(  # pylint: disable=protected-access
            url=(
                f"{self.scheme}://{self.primary_hostname}/"
                f"{kwargs.pop('path', '')}?{kwargs.pop('restype', '')}"
                f"comp=batch{kwargs.pop('sas', '')}{kwargs.pop('timeout', '')}"
            ),
            headers={"x-ms-version": self.api_version},
        )

        policies = [StorageHeadersPolicy()]
        if self._credential_policy:
            policies.append(self._credential_policy)  # type: ignore

        request.set_multipart_mixed(*reqs, policies=policies, enforce_https=False)

        pipeline_response = await self._pipeline.run(request, **kwargs)
        response = pipeline_response.http_response

        try:
            if response.status_code not in [202]:
                raise HttpResponseError(response=response)
            parts = response.parts()  # Return an AsyncIterator
            if raise_on_any_failure:
                parts_list = []
                async for part in parts:
                    parts_list.append(part)
                if any(p for p in parts_list if not 200 <= p.status_code < 300):
                    error = PartialBatchErrorException(
                        message="There is a partial failure in the batch operation.",
                        response=response,
                        parts=parts_list,
                    )
                    raise error
                return AsyncList(parts_list)
            return parts  # type: ignore [no-any-return]
        except HttpResponseError as error:
            process_storage_error(error)


def parse_connection_str(
    conn_str: str,
    credential: Optional[Union[str, Dict[str, str], AzureNamedKeyCredential, AzureSasCredential, AsyncTokenCredential]],
    service: str,
) -> Tuple[
    str,
    Optional[str],
    Optional[Union[str, Dict[str, str], AzureNamedKeyCredential, AzureSasCredential, AsyncTokenCredential]],
]:
    conn_str = conn_str.rstrip(";")
    conn_settings_list = [s.split("=", 1) for s in conn_str.split(";")]
    if any(len(tup) != 2 for tup in conn_settings_list):
        raise ValueError("Connection string is either blank or malformed.")
    conn_settings = dict((key.upper(), val) for key, val in conn_settings_list)
    endpoints = _SERVICE_PARAMS[service]
    primary = None
    secondary = None
    if not credential:
        try:
            credential = {"account_name": conn_settings["ACCOUNTNAME"], "account_key": conn_settings["ACCOUNTKEY"]}
        except KeyError:
            credential = conn_settings.get("SHAREDACCESSSIGNATURE")
    if endpoints["primary"] in conn_settings:
        primary = conn_settings[endpoints["primary"]]
        if endpoints["secondary"] in conn_settings:
            secondary = conn_settings[endpoints["secondary"]]
    else:
        if endpoints["secondary"] in conn_settings:
            raise ValueError("Connection string specifies only secondary endpoint.")
        try:
            primary = (
                f"{conn_settings['DEFAULTENDPOINTSPROTOCOL']}://"
                f"{conn_settings['ACCOUNTNAME']}.{service}.{conn_settings['ENDPOINTSUFFIX']}"
            )
            secondary = f"{conn_settings['ACCOUNTNAME']}-secondary." f"{service}.{conn_settings['ENDPOINTSUFFIX']}"
        except KeyError:
            pass

    if not primary:
        try:
            primary = (
                f"https://{conn_settings['ACCOUNTNAME']}."
                f"{service}.{conn_settings.get('ENDPOINTSUFFIX', SERVICE_HOST_BASE)}"
            )
        except KeyError as exc:
            raise ValueError("Connection string missing required connection details.") from exc
    if service == "dfs":
        primary = primary.replace(".blob.", ".dfs.")
        if secondary:
            secondary = secondary.replace(".blob.", ".dfs.")
    return primary, secondary, credential


class AsyncTransportWrapper(AsyncHttpTransport):
    """Wrapper class that ensures that an inner client created
    by a `get_client` method does not close the outer transport for the parent
    when used in a context manager.
    """

    def __init__(self, async_transport):
        self._transport = async_transport

    async def send(self, request, **kwargs):
        return await self._transport.send(request, **kwargs)

    async def open(self):
        pass

    async def close(self):
        pass

    async def __aenter__(self):
        pass

    async def __aexit__(self, *args):
        pass
