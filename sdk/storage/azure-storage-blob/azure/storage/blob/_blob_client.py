# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
# pylint: disable=too-many-lines, docstring-keyword-should-match-keyword-only

import warnings
from datetime import datetime
from functools import partial
from typing import (
    Any, AnyStr, cast, Dict, IO, Iterable, List, Optional, overload, Tuple, Union,
    TYPE_CHECKING
)
from typing_extensions import Self

from azure.core.exceptions import HttpResponseError, ResourceExistsError, ResourceNotFoundError
from azure.core.paging import ItemPaged
from azure.core.pipeline import Pipeline
from azure.core.tracing.decorator import distributed_trace
from ._blob_client_helpers import (
    _abort_copy_options,
    _append_block_from_url_options,
    _append_block_options,
    _clear_page_options,
    _commit_block_list_options,
    _create_append_blob_options,
    _create_page_blob_options,
    _create_snapshot_options,
    _delete_blob_options,
    _download_blob_options,
    _format_url,
    _from_blob_url,
    _get_blob_tags_options,
    _get_block_list_result,
    _get_page_ranges_options,
    _parse_url,
    _quick_query_options,
    _resize_blob_options,
    _seal_append_blob_options,
    _set_blob_metadata_options,
    _set_blob_tags_options,
    _set_http_headers_options,
    _set_sequence_number_options,
    _stage_block_from_url_options,
    _stage_block_options,
    _start_copy_from_url_options,
    _upload_blob_from_url_options,
    _upload_blob_options,
    _upload_page_options,
    _upload_pages_from_url_options
)
from ._deserialize import (
    deserialize_blob_properties,
    deserialize_pipeline_response_into_cls,
    get_page_ranges_result,
    parse_tags
)
from ._download import StorageStreamDownloader
from ._encryption import StorageEncryptionMixin, _ERROR_UNSUPPORTED_METHOD_FOR_ENCRYPTION
from ._generated import AzureBlobStorage
from ._generated.models import CpkInfo
from ._lease import BlobLeaseClient
from ._models import BlobBlock, BlobProperties, BlobQueryError, BlobType, PageRange, PageRangePaged
from ._quick_query_helper import BlobQueryReader
from ._shared.base_client import parse_connection_str, StorageAccountHostsMixin, TransportWrapper
from ._shared.response_handlers import process_storage_error, return_response_headers
from ._serialize import (
    get_access_conditions,
    get_api_version,
    get_modify_conditions,
    get_version_id
)
from ._upload_helpers import (
    upload_append_blob,
    upload_block_blob,
    upload_page_blob
)

if TYPE_CHECKING:
    from azure.core.credentials import AzureNamedKeyCredential, AzureSasCredential, TokenCredential
    from azure.storage.blob import ContainerClient
    from ._models import (
        ContentSettings,
        ImmutabilityPolicy,
        PremiumPageBlobTier,
        SequenceNumberAction,
        StandardBlobTier
    )


class BlobClient(StorageAccountHostsMixin, StorageEncryptionMixin):  # pylint: disable=too-many-public-methods
    """A client to interact with a specific blob, although that blob may not yet exist.

    For more optional configuration, please click
    `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
    #optional-configuration>`__.

    :param str account_url:
        The URI to the storage account. In order to create a client given the full URI to the blob,
        use the :func:`from_blob_url` classmethod.
    :param container_name: The container name for the blob.
    :type container_name: str
    :param blob_name: The name of the blob with which to interact. If specified, this value will override
        a blob value specified in the blob URL.
    :type blob_name: str
    :param str snapshot:
        The optional blob snapshot on which to operate. This can be the snapshot ID string
        or the response returned from :func:`create_snapshot`.
    :param credential:
        The credentials with which to authenticate. This is optional if the
        account URL already has a SAS token. The value can be a SAS token string,
        an instance of a AzureSasCredential or AzureNamedKeyCredential from azure.core.credentials,
        an account shared access key, or an instance of a TokenCredentials class from azure.identity.
        If the resource URI already contains a SAS token, this will be ignored in favor of an explicit credential
        - except in the case of AzureSasCredential, where the conflicting SAS tokens will raise a ValueError.
        If using an instance of AzureNamedKeyCredential, "name" should be the storage account name, and "key"
        should be the storage account key.
    :keyword str api_version:
        The Storage API version to use for requests. Default value is the most recent service version that is
        compatible with the current SDK. Setting to an older version may result in reduced feature compatibility.

        .. versionadded:: 12.2.0

    :keyword str secondary_hostname:
        The hostname of the secondary endpoint.
    :keyword int max_block_size: The maximum chunk size for uploading a block blob in chunks.
        Defaults to 4*1024*1024, or 4MB.
    :keyword int max_single_put_size: If the blob size is less than or equal max_single_put_size, then the blob will be
        uploaded with only one http PUT request. If the blob size is larger than max_single_put_size,
        the blob will be uploaded in chunks. Defaults to 64*1024*1024, or 64MB.
    :keyword int min_large_block_upload_threshold: The minimum chunk size required to use the memory efficient
        algorithm when uploading a block blob. Defaults to 4*1024*1024+1.
    :keyword bool use_byte_buffer: Use a byte buffer for block blob uploads. Defaults to False.
    :keyword int max_page_size: The maximum chunk size for uploading a page blob. Defaults to 4*1024*1024, or 4MB.
    :keyword int max_single_get_size: The maximum size for a blob to be downloaded in a single call,
        the exceeded part will be downloaded in chunks (could be parallel). Defaults to 32*1024*1024, or 32MB.
    :keyword int max_chunk_get_size: The maximum chunk size used for downloading a blob. Defaults to 4*1024*1024,
        or 4MB.
    :keyword str version_id: The version id parameter is an opaque DateTime value that, when present,
        specifies the version of the blob to operate on.
    :keyword str audience: The audience to use when requesting tokens for Azure Active Directory
        authentication. Only has an effect when credential is of type TokenCredential. The value could be
        https://storage.azure.com/ (default) or https://<account>.blob.core.windows.net.

    .. admonition:: Example:

        .. literalinclude:: ../samples/blob_samples_authentication.py
            :start-after: [START create_blob_client]
            :end-before: [END create_blob_client]
            :language: python
            :dedent: 8
            :caption: Creating the BlobClient from a URL to a public blob (no auth needed).

        .. literalinclude:: ../samples/blob_samples_authentication.py
            :start-after: [START create_blob_client_sas_url]
            :end-before: [END create_blob_client_sas_url]
            :language: python
            :dedent: 8
            :caption: Creating the BlobClient from a SAS URL to a blob.
    """
    def __init__(
        self, account_url: str,
        container_name: str,
        blob_name: str,
        snapshot: Optional[Union[str, Dict[str, Any]]] = None,
        credential: Optional[Union[str, Dict[str, str], "AzureNamedKeyCredential", "AzureSasCredential", "TokenCredential"]] = None,  # pylint: disable=line-too-long
        **kwargs: Any
    ) -> None:
        parsed_url, sas_token, path_snapshot = _parse_url(
            account_url=account_url,
            container_name=container_name,
            blob_name=blob_name)
        self.container_name = container_name
        self.blob_name = blob_name

        if snapshot is not None and hasattr(snapshot, 'snapshot'):
            self.snapshot = snapshot.snapshot
        elif isinstance(snapshot, dict):
            self.snapshot = snapshot['snapshot']
        else:
            self.snapshot = snapshot or path_snapshot
        self.version_id = kwargs.pop('version_id', None)

        # This parameter is used for the hierarchy traversal. Give precedence to credential.
        self._raw_credential = credential if credential else sas_token
        self._query_str, credential = self._format_query_string(sas_token, credential, snapshot=self.snapshot)
        super(BlobClient, self).__init__(parsed_url, service='blob', credential=credential, **kwargs)
        self._client = AzureBlobStorage(self.url, base_url=self.url, pipeline=self._pipeline)
        self._client._config.version = get_api_version(kwargs)  # type: ignore [assignment]
        self._configure_encryption(kwargs)

    def __enter__(self) -> Self:
        self._client.__enter__()
        return self

    def __exit__(self, *args) -> None:
        self._client.__exit__(*args)

    def close(self) -> None:
        """This method is to close the sockets opened by the client.
        It need not be used when using with a context manager.

        :return: None
        :rtype: None
        """
        self._client.close()

    def _format_url(self, hostname: str) -> str:
        return _format_url(
            container_name=self.container_name,
            scheme=self.scheme,
            blob_name=self.blob_name,
            query_str=self._query_str,
            hostname=hostname
        )

    @classmethod
    def from_blob_url(
        cls, blob_url: str,
        credential: Optional[Union[str, Dict[str, str], "AzureNamedKeyCredential", "AzureSasCredential", "TokenCredential"]] = None,  # pylint: disable=line-too-long
        snapshot: Optional[Union[str, Dict[str, Any]]] = None,
        **kwargs: Any
    ) -> Self:
        """Create BlobClient from a blob url. This doesn't support customized blob url with '/' in blob name.

        :param str blob_url:
            The full endpoint URL to the Blob, including SAS token and snapshot if used. This could be
            either the primary endpoint, or the secondary endpoint depending on the current `location_mode`.
        :type blob_url: str
        :param credential:
            The credentials with which to authenticate. This is optional if the
            account URL already has a SAS token, or the connection string already has shared
            access key values. The value can be a SAS token string,
            an instance of a AzureSasCredential or AzureNamedKeyCredential from azure.core.credentials,
            an account shared access key, or an instance of a TokenCredentials class from azure.identity.
            If the resource URI already contains a SAS token, this will be ignored in favor of an explicit credential
            - except in the case of AzureSasCredential, where the conflicting SAS tokens will raise a ValueError.
            If using an instance of AzureNamedKeyCredential, "name" should be the storage account name, and "key"
            should be the storage account key.
        :type credential:
            ~azure.core.credentials.AzureNamedKeyCredential or
            ~azure.core.credentials.AzureSasCredential or
            ~azure.core.credentials.TokenCredential or
            str or dict[str, str] or None
        :param str snapshot:
            The optional blob snapshot on which to operate. This can be the snapshot ID string
            or the response returned from :func:`create_snapshot`. If specified, this will override
            the snapshot in the url.
        :keyword str version_id: The version id parameter is an opaque DateTime value that, when present,
            specifies the version of the blob to operate on.
        :keyword str audience: The audience to use when requesting tokens for Azure Active Directory
            authentication. Only has an effect when credential is of type TokenCredential. The value could be
            https://storage.azure.com/ (default) or https://<account>.blob.core.windows.net.
        :return: A Blob client.
        :rtype: ~azure.storage.blob.BlobClient
        """
        account_url, container_name, blob_name, path_snapshot = _from_blob_url(blob_url=blob_url, snapshot=snapshot)
        return cls(
            account_url, container_name=container_name, blob_name=blob_name,
            snapshot=path_snapshot, credential=credential, **kwargs
        )

    @classmethod
    def from_connection_string(
        cls, conn_str: str,
        container_name: str,
        blob_name: str,
        snapshot: Optional[Union[str, Dict[str, Any]]] = None,
        credential: Optional[Union[str, Dict[str, str], "AzureNamedKeyCredential", "AzureSasCredential", "TokenCredential"]] = None,  # pylint: disable=line-too-long
        **kwargs: Any
    ) -> Self:
        """Create BlobClient from a Connection String.

        :param str conn_str:
            A connection string to an Azure Storage account.
        :param container_name: The container name for the blob.
        :type container_name: str
        :param blob_name: The name of the blob with which to interact.
        :type blob_name: str
        :param str snapshot:
            The optional blob snapshot on which to operate. This can be the snapshot ID string
            or the response returned from :func:`create_snapshot`.
        :param credential:
            The credentials with which to authenticate. This is optional if the
            account URL already has a SAS token, or the connection string already has shared
            access key values. The value can be a SAS token string,
            an instance of a AzureSasCredential or AzureNamedKeyCredential from azure.core.credentials,
            an account shared access key, or an instance of a TokenCredentials class from azure.identity.
            Credentials provided here will take precedence over those in the connection string.
            If using an instance of AzureNamedKeyCredential, "name" should be the storage account name, and "key"
            should be the storage account key.
        :type credential:
            ~azure.core.credentials.AzureNamedKeyCredential or
            ~azure.core.credentials.AzureSasCredential or
            ~azure.core.credentials.TokenCredential or
            str or dict[str, str] or None
        :keyword str version_id: The version id parameter is an opaque DateTime value that, when present,
            specifies the version of the blob to operate on.
        :keyword str audience: The audience to use when requesting tokens for Azure Active Directory
            authentication. Only has an effect when credential is of type TokenCredential. The value could be
            https://storage.azure.com/ (default) or https://<account>.blob.core.windows.net.
        :return: A Blob client.
        :rtype: ~azure.storage.blob.BlobClient

        .. admonition:: Example:

            .. literalinclude:: ../samples/blob_samples_authentication.py
                :start-after: [START auth_from_connection_string_blob]
                :end-before: [END auth_from_connection_string_blob]
                :language: python
                :dedent: 8
                :caption: Creating the BlobClient from a connection string.
        """
        account_url, secondary, credential = parse_connection_str(conn_str, credential, 'blob')
        if 'secondary_hostname' not in kwargs:
            kwargs['secondary_hostname'] = secondary
        return cls(
            account_url, container_name=container_name, blob_name=blob_name,
            snapshot=snapshot, credential=credential, **kwargs
        )

    @distributed_trace
    def get_account_information(self, **kwargs: Any) -> Dict[str, str]:
        """Gets information related to the storage account in which the blob resides.

        The information can also be retrieved if the user has a SAS to a container or blob.
        The keys in the returned dictionary include 'sku_name' and 'account_kind'.

        :return: A dict of account information (SKU and account type).
        :rtype: dict(str, str)
        """
        try:
            return cast(Dict[str, str], self._client.blob.get_account_info(cls=return_response_headers, **kwargs))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def upload_blob_from_url(
        self, source_url: str,
        *,
        metadata: Optional[Dict[str, str]] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Creates a new Block Blob where the content of the blob is read from a given URL.
        The content of an existing blob is overwritten with the new blob.

        :param str source_url:
            A URL of up to 2 KB in length that specifies a file or blob.
            The value should be URL-encoded as it would appear in a request URI.
            The source must either be public or must be authenticated via a shared
            access signature as part of the url or using the source_authorization keyword.
            If the source is public, no authentication is required.
            Examples:
            https://myaccount.blob.core.windows.net/mycontainer/myblob

            https://myaccount.blob.core.windows.net/mycontainer/myblob?snapshot=<DateTime>

            https://otheraccount.blob.core.windows.net/mycontainer/myblob?sastoken
        :keyword dict(str, str) metadata:
            Name-value pairs associated with the blob as metadata.
        :keyword bool overwrite: Whether the blob to be uploaded should overwrite the current data.
            If True, upload_blob will overwrite the existing data. If set to False, the
            operation will fail with ResourceExistsError.
        :keyword bool include_source_blob_properties:
            Indicates if properties from the source blob should be copied. Defaults to True.
        :keyword tags:
            Name-value pairs associated with the blob as tag. Tags are case-sensitive.
            The tag set may contain at most 10 tags.  Tag keys must be between 1 and 128 characters,
            and tag values must be between 0 and 256 characters.
            Valid tag key and value characters include: lowercase and uppercase letters, digits (0-9),
            space (' '), plus (+), minus (-), period (.), solidus (/), colon (:), equals (=), underscore (_)
        :paramtype tags: dict(str, str)
        :keyword bytearray source_content_md5:
            Specify the md5 that is used to verify the integrity of the source bytes.
        :keyword ~datetime.datetime source_if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the source resource has been modified since the specified time.
        :keyword ~datetime.datetime source_if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the source resource has not been modified since the specified date/time.
        :keyword str source_etag:
            The source ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions source_match_condition:
            The source match condition to use upon the etag.
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            The destination ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The destination match condition to use upon the etag.
        :keyword destination_lease:
            The lease ID specified for this header must match the lease ID of the
            destination blob. If the request does not include the lease ID or it is not
            valid, the operation fails with status code 412 (Precondition Failed).
        :paramtype destination_lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :keyword ~azure.storage.blob.ContentSettings content_settings:
            ContentSettings object used to set blob properties. Used to set content type, encoding,
            language, disposition, md5, and cache control.
        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the service. An encryption
            scope can be created using the Management API and referenced here by name. If a default
            encryption scope has been defined at the container, this value will override it if the
            container-level scope is configured to allow overrides. Otherwise an error will be raised.
        :keyword ~azure.storage.blob.StandardBlobTier standard_blob_tier:
            A standard blob tier value to set the blob to. For this version of the library,
            this is only applicable to block blobs on standard storage accounts.
        :keyword str source_authorization:
            Authenticate as a service principal using a client secret to access a source blob. Ensure "bearer " is
            the prefix of the source_authorization string.
        :keyword source_token_intent:
            Required when source is Azure Storage Files and using `TokenCredential` for authentication.
            This is ignored for other forms of authentication.
            Specifies the intent for all requests when using `TokenCredential` authentication. Possible values are:

            backup - Specifies requests are intended for backup/admin type operations, meaning that all file/directory
                 ACLs are bypassed and full permissions are granted. User must also have required RBAC permission.

        :paramtype source_token_intent: Literal['backup']
        :return: Blob-updated property Dict (Etag and last modified)
        :rtype: Dict[str, Any]
        """
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _upload_blob_from_url_options(
            source_url=source_url,
            metadata=metadata,
            **kwargs
        )
        try:
            return cast(Dict[str, Any], self._client.block_blob.put_blob_from_url(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def upload_blob(
        self, data: Union[bytes, str, Iterable[AnyStr], IO[bytes]],
        blob_type: Union[str, BlobType] = BlobType.BLOCKBLOB,
        length: Optional[int] = None,
        metadata: Optional[Dict[str, str]] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """Creates a new blob from a data source with automatic chunking.

        :param data: The blob data to upload.
        :type data: Union[bytes, str, Iterable[AnyStr], IO[AnyStr]]
        :param ~azure.storage.blob.BlobType blob_type: The type of the blob. This can be
            either BlockBlob, PageBlob or AppendBlob. The default value is BlockBlob.
        :param int length:
            Number of bytes to read from the stream. This is optional, but
            should be supplied for optimal performance.
        :param metadata:
            Name-value pairs associated with the blob as metadata.
        :type metadata: dict(str, str)
        :keyword tags:
            Name-value pairs associated with the blob as tag. Tags are case-sensitive.
            The tag set may contain at most 10 tags.  Tag keys must be between 1 and 128 characters,
            and tag values must be between 0 and 256 characters.
            Valid tag key and value characters include: lowercase and uppercase letters, digits (0-9),
            space (' '), plus (+), minus (-), period (.), solidus (/), colon (:), equals (=), underscore (_)

            .. versionadded:: 12.4.0

        :paramtype tags: dict(str, str)
        :keyword bool overwrite: Whether the blob to be uploaded should overwrite the current data.
            If True, upload_blob will overwrite the existing data. If set to False, the
            operation will fail with ResourceExistsError. The exception to the above is with Append
            blob types: if set to False and the data already exists, an error will not be raised
            and the data will be appended to the existing blob. If set overwrite=True, then the existing
            append blob will be deleted, and a new one created. Defaults to False.
        :keyword ~azure.storage.blob.ContentSettings content_settings:
            ContentSettings object used to set blob properties. Used to set content type, encoding,
            language, disposition, md5, and cache control.
        :keyword bool validate_content:
            If true, calculates an MD5 hash for each chunk of the blob. The storage
            service checks the hash of the content that has arrived with the hash
            that was sent. This is primarily valuable for detecting bitflips on
            the wire if using http instead of https, as https (the default), will
            already validate. Note that this MD5 hash is not stored with the
            blob. Also note that if enabled, the memory-efficient upload algorithm
            will not be used because computing the MD5 hash requires buffering
            entire blocks, and doing so defeats the purpose of the memory-efficient algorithm.
        :keyword lease:
            Required if the blob has an active lease. If specified, upload_blob only succeeds if the
            blob's lease is active and matches this ID. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword ~azure.storage.blob.PremiumPageBlobTier premium_page_blob_tier:
            A page blob tier value to set the blob to. The tier correlates to the size of the
            blob and number of allowed IOPS. This is only applicable to page blobs on
            premium storage accounts.
        :keyword ~azure.storage.blob.StandardBlobTier standard_blob_tier:
            A standard blob tier value to set the blob to. For this version of the library,
            this is only applicable to block blobs on standard storage accounts.
        :keyword ~azure.storage.blob.ImmutabilityPolicy immutability_policy:
            Specifies the immutability policy of a blob, blob snapshot or blob version.
            Currently this parameter of upload_blob() API is for BlockBlob only.

            .. versionadded:: 12.10.0
                This was introduced in API version '2020-10-02'.

        :keyword bool legal_hold:
            Specified if a legal hold should be set on the blob.
            Currently this parameter of upload_blob() API is for BlockBlob only.

            .. versionadded:: 12.10.0
                This was introduced in API version '2020-10-02'.

        :keyword int maxsize_condition:
            Optional conditional header. The max length in bytes permitted for
            the append blob. If the Append Block operation would cause the blob
            to exceed that limit or if the blob size is already greater than the
            value specified in this header, the request will fail with
            MaxBlobSizeConditionNotMet error (HTTP status code 412 - Precondition Failed).
        :keyword int max_concurrency:
            Maximum number of parallel connections to use when transferring the blob in chunks.
            This option does not affect the underlying connection pool, and may
            require a separate configuration of the connection pool.
        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the service. An encryption
            scope can be created using the Management API and referenced here by name. If a default
            encryption scope has been defined at the container, this value will override it if the
            container-level scope is configured to allow overrides. Otherwise an error will be raised.

            .. versionadded:: 12.2.0

        :keyword str encoding:
            Defaults to UTF-8.
        :keyword progress_hook:
            A callback to track the progress of a long running upload. The signature is
            function(current: int, total: Optional[int]) where current is the number of bytes transferred
            so far, and total is the size of the blob or None if the size is unknown.
        :paramtype progress_hook: Callable[[int, Optional[int]], None]
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__. This method may make multiple calls to the service and
            the timeout will apply to each call individually.
        :return: Blob-updated property Dict (Etag and last modified)
        :rtype: Dict[str, Any]

        .. admonition:: Example:

            .. literalinclude:: ../samples/blob_samples_hello_world.py
                :start-after: [START upload_a_blob]
                :end-before: [END upload_a_blob]
                :language: python
                :dedent: 12
                :caption: Upload a blob to the container.
        """
        if self.require_encryption and not self.key_encryption_key:
            raise ValueError("Encryption required but no key was provided.")
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _upload_blob_options(
            data=data,
            blob_type=blob_type,
            length=length,
            metadata=metadata,
            encryption_options={
                'required': self.require_encryption,
                'version': self.encryption_version,
                'key': self.key_encryption_key,
                'resolver': self.key_resolver_function
            },
            config=self._config,
            sdk_moniker=self._sdk_moniker,
            client=self._client,
            **kwargs)
        if blob_type == BlobType.BlockBlob:
            return upload_block_blob(**options)
        if blob_type == BlobType.PageBlob:
            return upload_page_blob(**options)
        return upload_append_blob(**options)

    @overload
    def download_blob(
        self, offset: Optional[int] = None,
        length: Optional[int] = None,
        *,
        encoding: str,
        **kwargs: Any
    ) -> StorageStreamDownloader[str]:
        ...

    @overload
    def download_blob(
        self, offset: Optional[int] = None,
        length: Optional[int] = None,
        *,
        encoding: None = None,
        **kwargs: Any
    ) -> StorageStreamDownloader[bytes]:
        ...

    @distributed_trace
    def download_blob(
        self, offset: Optional[int] = None,
        length: Optional[int] = None,
        *,
        encoding: Union[str, None] = None,
        **kwargs: Any
    ) -> Union[StorageStreamDownloader[str], StorageStreamDownloader[bytes]]:
        """Downloads a blob to the StorageStreamDownloader. The readall() method must
        be used to read all the content or readinto() must be used to download the blob into
        a stream. Using chunks() returns an iterator which allows the user to iterate over the content in chunks.

        :param int offset:
            Start of byte range to use for downloading a section of the blob.
            Must be set if length is provided.
        :param int length:
            Number of bytes to read from the stream. This is optional, but
            should be supplied for optimal performance.
        :keyword str version_id:
            The version id parameter is an opaque DateTime
            value that, when present, specifies the version of the blob to download.

            .. versionadded:: 12.4.0

            This keyword argument was introduced in API version '2019-12-12'.

        :keyword bool validate_content:
            If true, calculates an MD5 hash for each chunk of the blob. The storage
            service checks the hash of the content that has arrived with the hash
            that was sent. This is primarily valuable for detecting bitflips on
            the wire if using http instead of https, as https (the default), will
            already validate. Note that this MD5 hash is not stored with the
            blob. Also note that if enabled, the memory-efficient upload algorithm
            will not be used because computing the MD5 hash requires buffering
            entire blocks, and doing so defeats the purpose of the memory-efficient algorithm.
        :keyword lease:
            Required if the blob has an active lease. If specified, download_blob only
            succeeds if the blob's lease is active and matches this ID. Value can be a
            BlobLeaseClient object or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword int max_concurrency:
            Maximum number of parallel connections to use when transferring the blob in chunks.
            This option does not affect the underlying connection pool, and may
            require a separate configuration of the connection pool.
        :keyword Optional[str] encoding:
            Encoding to decode the downloaded bytes. Default is None, i.e. no decoding.
        :keyword progress_hook:
            A callback to track the progress of a long running download. The signature is
            function(current: int, total: int) where current is the number of bytes transferred
            so far, and total is the total size of the download.
        :paramtype progress_hook: Callable[[int, int], None]
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__. This method may make multiple calls to the service and
            the timeout will apply to each call individually.
            multiple calls to the Azure service and the timeout will apply to
            each call individually.
        :return: A streaming object (StorageStreamDownloader)
        :rtype: ~azure.storage.blob.StorageStreamDownloader

        .. admonition:: Example:

            .. literalinclude:: ../samples/blob_samples_hello_world.py
                :start-after: [START download_a_blob]
                :end-before: [END download_a_blob]
                :language: python
                :dedent: 12
                :caption: Download a blob.
        """
        if self.require_encryption and not (self.key_encryption_key or self.key_resolver_function):
            raise ValueError("Encryption required but no key was provided.")
        if length is not None and offset is None:
            raise ValueError("Offset value must not be None if length is set.")
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _download_blob_options(
            blob_name=self.blob_name,
            container_name=self.container_name,
            version_id=get_version_id(self.version_id, kwargs),
            offset=offset,
            length=length,
            encoding=encoding,
            encryption_options={
                'required': self.require_encryption,
                'version': self.encryption_version,
                'key': self.key_encryption_key,
                'resolver': self.key_resolver_function
            },
            config=self._config,
            sdk_moniker=self._sdk_moniker,
            client=self._client,
            **kwargs)
        return StorageStreamDownloader(**options)

    @distributed_trace
    def query_blob(self, query_expression: str, **kwargs: Any) -> BlobQueryReader:
        """Enables users to select/project on blob/or blob snapshot data by providing simple query expressions.
        This operations returns a BlobQueryReader, users need to use readall() or readinto() to get query data.

        :param str query_expression:
            Required. a query statement. For more details see
            https://learn.microsoft.com/azure/storage/blobs/query-acceleration-sql-reference.
        :keyword Callable[~azure.storage.blob.BlobQueryError] on_error:
            A function to be called on any processing errors returned by the service.
        :keyword blob_format:
            Optional. Defines the serialization of the data currently stored in the blob. The default is to
            treat the blob data as CSV data formatted in the default dialect. This can be overridden with
            a custom DelimitedTextDialect, or DelimitedJsonDialect or "ParquetDialect" (passed as a string or enum).
            These dialects can be passed through their respective classes, the QuickQueryDialect enum or as a string

            .. note::
                "ParquetDialect" is in preview, so some features may not work as intended.

        :paramtype blob_format: ~azure.storage.blob.DelimitedTextDialect or ~azure.storage.blob.DelimitedJsonDialect
            or ~azure.storage.blob.QuickQueryDialect or str
        :keyword output_format:
            Optional. Defines the output serialization for the data stream. By default the data will be returned
            as it is represented in the blob (Parquet formats default to DelimitedTextDialect).
            By providing an output format, the blob data will be reformatted according to that profile.
            This value can be a DelimitedTextDialect or a DelimitedJsonDialect or ArrowDialect.
            These dialects can be passed through their respective classes, the QuickQueryDialect enum or as a string
        :paramtype output_format: ~azure.storage.blob.DelimitedTextDialect or ~azure.storage.blob.DelimitedJsonDialect
            or List[~azure.storage.blob.ArrowDialect] or ~azure.storage.blob.QuickQueryDialect or str
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: A streaming object (BlobQueryReader)
        :rtype: ~azure.storage.blob.BlobQueryReader

        .. admonition:: Example:

            .. literalinclude:: ../samples/blob_samples_query.py
                :start-after: [START query]
                :end-before: [END query]
                :language: python
                :dedent: 4
                :caption: select/project on blob/or blob snapshot data by providing simple query expressions.
        """
        errors = kwargs.pop("on_error", None)
        error_cls = kwargs.pop("error_cls", BlobQueryError)
        encoding = kwargs.pop("encoding", None)
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options, delimiter = _quick_query_options(self.snapshot, query_expression, **kwargs)
        try:
            headers, raw_response_body = self._client.blob.query(**options)
        except HttpResponseError as error:
            process_storage_error(error)
        return BlobQueryReader(
            name=self.blob_name,
            container=self.container_name,
            errors=errors,
            record_delimiter=delimiter,
            encoding=encoding,
            headers=headers,
            response=raw_response_body,
            error_cls=error_cls)

    @distributed_trace
    def delete_blob(self, delete_snapshots: Optional[str] = None, **kwargs: Any) -> None:
        """Marks the specified blob for deletion.

        The blob is later deleted during garbage collection.
        Note that in order to delete a blob, you must delete all of its
        snapshots. You can delete both at the same time with the delete_blob()
        operation.

        If a delete retention policy is enabled for the service, then this operation soft deletes the blob
        and retains the blob for a specified number of days.
        After the specified number of days, the blob's data is removed from the service during garbage collection.
        Soft deleted blob is accessible through :func:`~ContainerClient.list_blobs()` specifying `include=['deleted']`
        option. Soft-deleted blob can be restored using :func:`~BlobClient.undelete_blob()` operation.

        :param Optional[str] delete_snapshots:
            Required if the blob has associated snapshots. Values include:
             - "only": Deletes only the blobs snapshots.
             - "include": Deletes the blob along with all snapshots.
        :keyword Optional[str] version_id:
            The version id parameter is an opaque DateTime
            value that, when present, specifies the version of the blob to delete.

            .. versionadded:: 12.4.0

            This keyword argument was introduced in API version '2019-12-12'.

        :keyword lease:
            Required if the blob has an active lease. If specified, delete_blob only
            succeeds if the blob's lease is active and matches this ID. Value can be a
            BlobLeaseClient object or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: None
        :rtype: None

        .. admonition:: Example:

            .. literalinclude:: ../samples/blob_samples_hello_world.py
                :start-after: [START delete_blob]
                :end-before: [END delete_blob]
                :language: python
                :dedent: 12
                :caption: Delete a blob.
        """
        options = _delete_blob_options(
            snapshot=self.snapshot,
            version_id=get_version_id(self.version_id, kwargs),
            delete_snapshots=delete_snapshots,
            **kwargs)
        try:
            self._client.blob.delete(**options)
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def undelete_blob(self, **kwargs: Any) -> None:
        """Restores soft-deleted blobs or snapshots.

        Operation will only be successful if used within the specified number of days
        set in the delete retention policy.

        If blob versioning is enabled, the base blob cannot be restored using this
        method. Instead use :func:`start_copy_from_url` with the URL of the blob version
        you wish to promote to the current version.

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: None
        :rtype: None

        .. admonition:: Example:

            .. literalinclude:: ../samples/blob_samples_common.py
                :start-after: [START undelete_blob]
                :end-before: [END undelete_blob]
                :language: python
                :dedent: 8
                :caption: Undeleting a blob.
        """
        try:
            self._client.blob.undelete(timeout=kwargs.pop('timeout', None), **kwargs)
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def exists(self, **kwargs: Any) -> bool:
        """
        Returns True if a blob exists with the defined parameters, and returns
        False otherwise.

        :keyword str version_id:
            The version id parameter is an opaque DateTime
            value that, when present, specifies the version of the blob to check if it exists.
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: boolean
        :rtype: bool
        """
        version_id = get_version_id(self.version_id, kwargs)
        try:
            self._client.blob.get_properties(
                snapshot=self.snapshot,
                version_id=version_id,
                **kwargs)
            return True
        # Encrypted with CPK
        except ResourceExistsError:
            return True
        except HttpResponseError as error:
            try:
                process_storage_error(error)
            except ResourceNotFoundError:
                return False

    @distributed_trace
    def get_blob_properties(self, **kwargs: Any) -> BlobProperties:
        """Returns all user-defined metadata, standard HTTP properties, and
        system properties for the blob. It does not return the content of the blob.

        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword str version_id:
            The version id parameter is an opaque DateTime
            value that, when present, specifies the version of the blob to get properties.

            .. versionadded:: 12.4.0

            This keyword argument was introduced in API version '2019-12-12'.

        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: BlobProperties
        :rtype: ~azure.storage.blob.BlobProperties

        .. admonition:: Example:

            .. literalinclude:: ../samples/blob_samples_common.py
                :start-after: [START get_blob_properties]
                :end-before: [END get_blob_properties]
                :language: python
                :dedent: 8
                :caption: Getting the properties for a blob.
        """
        # TODO: extract this out as _get_blob_properties_options
        access_conditions = get_access_conditions(kwargs.pop('lease', None))
        mod_conditions = get_modify_conditions(kwargs)
        version_id = get_version_id(self.version_id, kwargs)
        cpk = kwargs.pop('cpk', None)
        cpk_info = None
        if cpk:
            if self.scheme.lower() != 'https':
                raise ValueError("Customer provided encryption key must be used over HTTPS.")
            cpk_info = CpkInfo(encryption_key=cpk.key_value, encryption_key_sha256=cpk.key_hash,
                               encryption_algorithm=cpk.algorithm)
        try:
            cls_method = kwargs.pop('cls', None)
            if cls_method:
                kwargs['cls'] = partial(deserialize_pipeline_response_into_cls, cls_method)
            blob_props = cast(BlobProperties, self._client.blob.get_properties(
                timeout=kwargs.pop('timeout', None),
                version_id=version_id,
                snapshot=self.snapshot,
                lease_access_conditions=access_conditions,
                modified_access_conditions=mod_conditions,
                cls=kwargs.pop('cls', None) or deserialize_blob_properties,
                cpk_info=cpk_info,
                **kwargs))
        except HttpResponseError as error:
            process_storage_error(error)
        blob_props.name = self.blob_name
        if isinstance(blob_props, BlobProperties):
            blob_props.container = self.container_name
            blob_props.snapshot = self.snapshot
        return blob_props

    @distributed_trace
    def set_http_headers(self, content_settings: Optional["ContentSettings"] = None, **kwargs: Any) -> Dict[str, Any]:
        """Sets system properties on the blob.

        If one property is set for the content_settings, all properties will be overridden.

        :param ~azure.storage.blob.ContentSettings content_settings:
            ContentSettings object used to set blob properties. Used to set content type, encoding,
            language, disposition, md5, and cache control.
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Blob-updated property dict (Etag and last modified)
        :rtype: Dict[str, Any]
        """
        options = _set_http_headers_options(content_settings=content_settings, **kwargs)
        try:
            return cast(Dict[str, Any], self._client.blob.set_http_headers(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def set_blob_metadata(
        self, metadata: Optional[Dict[str, str]] = None,
        **kwargs: Any
    ) -> Dict[str, Union[str, datetime]]:
        """Sets user-defined metadata for the blob as one or more name-value pairs.

        :param metadata:
            Dict containing name and value pairs. Each call to this operation
            replaces all existing metadata attached to the blob. To remove all
            metadata from the blob, call this operation with no metadata headers.
        :type metadata: dict(str, str)
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the service. An encryption
            scope can be created using the Management API and referenced here by name. If a default
            encryption scope has been defined at the container, this value will override it if the
            container-level scope is configured to allow overrides. Otherwise an error will be raised.

            .. versionadded:: 12.2.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Blob-updated property dict (Etag and last modified)
        :rtype: Dict[str, Union[str, datetime]]
        """
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _set_blob_metadata_options(metadata=metadata, **kwargs)
        try:
            return cast(Dict[str, Union[str, datetime]], self._client.blob.set_metadata(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def set_immutability_policy(
        self, immutability_policy: "ImmutabilityPolicy",
        **kwargs: Any
    ) -> Dict[str, str]:
        """The Set Immutability Policy operation sets the immutability policy on the blob.

        .. versionadded:: 12.10.0
            This operation was introduced in API version '2020-10-02'.

        :param ~azure.storage.blob.ImmutabilityPolicy immutability_policy:
            Specifies the immutability policy of a blob, blob snapshot or blob version.

            .. versionadded:: 12.10.0
                This was introduced in API version '2020-10-02'.

        :keyword str version_id:
            The version id parameter is an opaque DateTime
            value that, when present, specifies the version of the blob to check if it exists.
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Key value pairs of blob tags.
        :rtype: Dict[str, str]
        """

        version_id = get_version_id(self.version_id, kwargs)
        kwargs['immutability_policy_expiry'] = immutability_policy.expiry_time
        kwargs['immutability_policy_mode'] = immutability_policy.policy_mode
        return cast(Dict[str, str], self._client.blob.set_immutability_policy(
            cls=return_response_headers, version_id=version_id, **kwargs))

    @distributed_trace
    def delete_immutability_policy(self, **kwargs: Any) -> None:
        """The Delete Immutability Policy operation deletes the immutability policy on the blob.

        .. versionadded:: 12.10.0
            This operation was introduced in API version '2020-10-02'.

        :keyword str version_id:
            The version id parameter is an opaque DateTime
            value that, when present, specifies the version of the blob to check if it exists.
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Key value pairs of blob tags.
        :rtype: Dict[str, str]
        """

        version_id = get_version_id(self.version_id, kwargs)
        self._client.blob.delete_immutability_policy(version_id=version_id, **kwargs)

    @distributed_trace
    def set_legal_hold(self, legal_hold: bool, **kwargs: Any) -> Dict[str, Union[str, datetime, bool]]:
        """The Set Legal Hold operation sets a legal hold on the blob.

        .. versionadded:: 12.10.0
            This operation was introduced in API version '2020-10-02'.

        :param bool legal_hold:
            Specified if a legal hold should be set on the blob.
        :keyword str version_id:
            The version id parameter is an opaque DateTime
            value that, when present, specifies the version of the blob to check if it exists.
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Key value pairs of blob tags.
        :rtype: Dict[str, Union[str, datetime, bool]]
        """

        version_id = get_version_id(self.version_id, kwargs)
        return cast(Dict[str, Union[str, datetime, bool]], self._client.blob.set_legal_hold(
            legal_hold, version_id=version_id, cls=return_response_headers, **kwargs))

    @distributed_trace
    def create_page_blob(
        self, size: int,
        content_settings: Optional["ContentSettings"] = None,
        metadata: Optional[Dict[str, str]] = None,
        premium_page_blob_tier: Optional[Union[str, "PremiumPageBlobTier"]] = None,
        **kwargs: Any
    ) -> Dict[str, Union[str, datetime]]:
        """Creates a new Page Blob of the specified size.

        :param int size:
            This specifies the maximum size for the page blob, up to 1 TB.
            The page blob size must be aligned to a 512-byte boundary.
        :param ~azure.storage.blob.ContentSettings content_settings:
            ContentSettings object used to set blob properties. Used to set content type, encoding,
            language, disposition, md5, and cache control.
        :param metadata:
            Name-value pairs associated with the blob as metadata.
        :type metadata: dict(str, str)
        :param ~azure.storage.blob.PremiumPageBlobTier premium_page_blob_tier:
            A page blob tier value to set the blob to. The tier correlates to the size of the
            blob and number of allowed IOPS. This is only applicable to page blobs on
            premium storage accounts.
        :keyword tags:
            Name-value pairs associated with the blob as tag. Tags are case-sensitive.
            The tag set may contain at most 10 tags.  Tag keys must be between 1 and 128 characters,
            and tag values must be between 0 and 256 characters.
            Valid tag key and value characters include: lowercase and uppercase letters, digits (0-9),
            space (' '), plus (+), minus (-), period (.), solidus (/), colon (:), equals (=), underscore (_)

            .. versionadded:: 12.4.0

        :paramtype tags: dict(str, str)
        :keyword int sequence_number:
            Only for Page blobs. The sequence number is a user-controlled value that you can use to
            track requests. The value of the sequence number must be between 0
            and 2^63 - 1.The default value is 0.
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~azure.storage.blob.ImmutabilityPolicy immutability_policy:
            Specifies the immutability policy of a blob, blob snapshot or blob version.

            .. versionadded:: 12.10.0
                This was introduced in API version '2020-10-02'.

        :keyword bool legal_hold:
            Specified if a legal hold should be set on the blob.

            .. versionadded:: 12.10.0
                This was introduced in API version '2020-10-02'.

        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the service. An encryption
            scope can be created using the Management API and referenced here by name. If a default
            encryption scope has been defined at the container, this value will override it if the
            container-level scope is configured to allow overrides. Otherwise an error will be raised.

            .. versionadded:: 12.2.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Blob-updated property dict (Etag and last modified).
        :rtype: dict[str, Any]
        """
        if self.require_encryption or (self.key_encryption_key is not None):
            raise ValueError(_ERROR_UNSUPPORTED_METHOD_FOR_ENCRYPTION)
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _create_page_blob_options(
            size=size,
            content_settings=content_settings,
            metadata=metadata,
            premium_page_blob_tier=premium_page_blob_tier,
            **kwargs)
        try:
            return cast(Dict[str, Any], self._client.page_blob.create(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def create_append_blob(
        self, content_settings: Optional["ContentSettings"] = None,
        metadata: Optional[Dict[str, str]] = None,
        **kwargs: Any
    ) -> Dict[str, Union[str, datetime]]:
        """Creates a new Append Blob. This operation creates a new 0-length append blob. The content
        of any existing blob is overwritten with the newly initialized append blob. To add content to
        the append blob, call the :func:`append_block` or :func:`append_block_from_url` method.

        :param ~azure.storage.blob.ContentSettings content_settings:
            ContentSettings object used to set blob properties. Used to set content type, encoding,
            language, disposition, md5, and cache control.
        :param metadata:
            Name-value pairs associated with the blob as metadata.
        :type metadata: dict(str, str)
        :keyword tags:
            Name-value pairs associated with the blob as tag. Tags are case-sensitive.
            The tag set may contain at most 10 tags.  Tag keys must be between 1 and 128 characters,
            and tag values must be between 0 and 256 characters.
            Valid tag key and value characters include: lowercase and uppercase letters, digits (0-9),
            space (' '), plus (+), minus (-), period (.), solidus (/), colon (:), equals (=), underscore (_)

            .. versionadded:: 12.4.0

        :paramtype tags: dict(str, str)
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~azure.storage.blob.ImmutabilityPolicy immutability_policy:
            Specifies the immutability policy of a blob, blob snapshot or blob version.

            .. versionadded:: 12.10.0
                This was introduced in API version '2020-10-02'.

        :keyword bool legal_hold:
            Specified if a legal hold should be set on the blob.

            .. versionadded:: 12.10.0
                This was introduced in API version '2020-10-02'.

        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the service. An encryption
            scope can be created using the Management API and referenced here by name. If a default
            encryption scope has been defined at the container, this value will override it if the
            container-level scope is configured to allow overrides. Otherwise an error will be raised.

            .. versionadded:: 12.2.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Blob-updated property dict (Etag and last modified).
        :rtype: dict[str, Any]
        """
        if self.require_encryption or (self.key_encryption_key is not None):
            raise ValueError(_ERROR_UNSUPPORTED_METHOD_FOR_ENCRYPTION)
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _create_append_blob_options(
            content_settings=content_settings,
            metadata=metadata,
            **kwargs)
        try:
            return cast(Dict[str, Union[str, datetime]], self._client.append_blob.create(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def create_snapshot(
        self, metadata: Optional[Dict[str, str]] = None,
        **kwargs: Any
    ) -> Dict[str, Union[str, datetime]]:
        """Creates a snapshot of the blob.

        A snapshot is a read-only version of a blob that's taken at a point in time.
        It can be read, copied, or deleted, but not modified. Snapshots provide a way
        to back up a blob as it appears at a moment in time.

        A snapshot of a blob has the same name as the base blob from which the snapshot
        is taken, with a DateTime value appended to indicate the time at which the
        snapshot was taken.

        :param metadata:
            Name-value pairs associated with the blob as metadata.
        :type metadata: dict(str, str)
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on destination blob with a matching value.

            .. versionadded:: 12.4.0

        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the service. An encryption
            scope can be created using the Management API and referenced here by name. If a default
            encryption scope has been defined at the container, this value will override it if the
            container-level scope is configured to allow overrides. Otherwise an error will be raised.

            .. versionadded:: 12.2.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Blob-updated property dict (Snapshot ID, Etag, and last modified).
        :rtype: dict[str, Any]

        .. admonition:: Example:

            .. literalinclude:: ../samples/blob_samples_common.py
                :start-after: [START create_blob_snapshot]
                :end-before: [END create_blob_snapshot]
                :language: python
                :dedent: 8
                :caption: Create a snapshot of the blob.
        """
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _create_snapshot_options(metadata=metadata, **kwargs)
        try:
            return cast(Dict[str, Any], self._client.blob.create_snapshot(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def start_copy_from_url(
        self, source_url: str,
        metadata: Optional[Dict[str, str]] = None,
        incremental_copy: bool = False,
        **kwargs: Any
    ) -> Dict[str, Union[str, datetime]]:
        """Copies a blob from the given URL.

        This operation returns a dictionary containing `copy_status` and `copy_id`,
        which can be used to check the status of or abort the copy operation.
        `copy_status` will be 'success' if the copy completed synchronously or
        'pending' if the copy has been started asynchronously. For asynchronous copies,
        the status can be checked by polling the :func:`get_blob_properties` method and
        checking the copy status. Set `requires_sync` to True to force the copy to be synchronous.
        The Blob service copies blobs on a best-effort basis.

        The source blob for a copy operation may be a block blob, an append blob,
        or a page blob. If the destination blob already exists, it must be of the
        same blob type as the source blob. Any existing destination blob will be
        overwritten. The destination blob cannot be modified while a copy operation
        is in progress.

        When copying from a page blob, the Blob service creates a destination page
        blob of the source blob's length, initially containing all zeroes. Then
        the source page ranges are enumerated, and non-empty ranges are copied.

        For a block blob or an append blob, the Blob service creates a committed
        blob of zero length before returning from this operation. When copying
        from a block blob, all committed blocks and their block IDs are copied.
        Uncommitted blocks are not copied. At the end of the copy operation, the
        destination blob will have the same committed block count as the source.

        When copying from an append blob, all committed blocks are copied. At the
        end of the copy operation, the destination blob will have the same committed
        block count as the source.

        :param str source_url:
            A URL of up to 2 KB in length that specifies a file or blob.
            The value should be URL-encoded as it would appear in a request URI.
            If the source is in another account, the source must either be public
            or must be authenticated via a shared access signature. If the source
            is public, no authentication is required.
            Examples:
            https://myaccount.blob.core.windows.net/mycontainer/myblob

            https://myaccount.blob.core.windows.net/mycontainer/myblob?snapshot=<DateTime>

            https://otheraccount.blob.core.windows.net/mycontainer/myblob?sastoken
        :param metadata:
            Name-value pairs associated with the blob as metadata. If no name-value
            pairs are specified, the operation will copy the metadata from the
            source blob or file to the destination blob. If one or more name-value
            pairs are specified, the destination blob is created with the specified
            metadata, and metadata is not copied from the source blob or file.
        :type metadata: dict(str, str)
        :param bool incremental_copy:
            Copies the snapshot of the source page blob to a destination page blob.
            The snapshot is copied such that only the differential changes between
            the previously copied snapshot are transferred to the destination.
            The copied snapshots are complete copies of the original snapshot and
            can be read or copied from as usual. Defaults to False.
        :keyword tags:
            Name-value pairs associated with the blob as tag. Tags are case-sensitive.
            The tag set may contain at most 10 tags.  Tag keys must be between 1 and 128 characters,
            and tag values must be between 0 and 256 characters.
            Valid tag key and value characters include: lowercase and uppercase letters, digits (0-9),
            space (' '), plus (+), minus (-), period (.), solidus (/), colon (:), equals (=), underscore (_).

            The (case-sensitive) literal "COPY" can instead be passed to copy tags from the source blob.
            This option is only available when `incremental_copy=False` and `requires_sync=True`.

            .. versionadded:: 12.4.0

        :paramtype tags: dict(str, str) or Literal["COPY"]
        :keyword ~azure.storage.blob.ImmutabilityPolicy immutability_policy:
            Specifies the immutability policy of a blob, blob snapshot or blob version.

            .. versionadded:: 12.10.0
                This was introduced in API version '2020-10-02'.

        :keyword bool legal_hold:
            Specified if a legal hold should be set on the blob.

            .. versionadded:: 12.10.0
                This was introduced in API version '2020-10-02'.

        :keyword ~datetime.datetime source_if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this conditional header to copy the blob only if the source
            blob has been modified since the specified date/time.
        :keyword ~datetime.datetime source_if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this conditional header to copy the blob only if the source blob
            has not been modified since the specified date/time.
        :keyword str source_etag:
            The source ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions source_match_condition:
            The source match condition to use upon the etag.
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this conditional header to copy the blob only
            if the destination blob has been modified since the specified date/time.
            If the destination blob has not been modified, the Blob service returns
            status code 412 (Precondition Failed).
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this conditional header to copy the blob only
            if the destination blob has not been modified since the specified
            date/time. If the destination blob has been modified, the Blob service
            returns status code 412 (Precondition Failed).
        :keyword str etag:
            The destination ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The destination match condition to use upon the etag.
        :keyword destination_lease:
            The lease ID specified for this header must match the lease ID of the
            destination blob. If the request does not include the lease ID or it is not
            valid, the operation fails with status code 412 (Precondition Failed).
        :paramtype destination_lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword source_lease:
            Specify this to perform the Copy Blob operation only if
            the lease ID given matches the active lease ID of the source blob.
        :paramtype source_lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :keyword ~azure.storage.blob.PremiumPageBlobTier premium_page_blob_tier:
            A page blob tier value to set the blob to. The tier correlates to the size of the
            blob and number of allowed IOPS. This is only applicable to page blobs on
            premium storage accounts.
        :keyword ~azure.storage.blob.StandardBlobTier standard_blob_tier:
            A standard blob tier value to set the blob to. For this version of the library,
            this is only applicable to block blobs on standard storage accounts.
        :keyword ~azure.storage.blob.RehydratePriority rehydrate_priority:
            Indicates the priority with which to rehydrate an archived blob
        :keyword bool seal_destination_blob:
            Seal the destination append blob. This operation is only for append blob.

            .. versionadded:: 12.4.0

        :keyword bool requires_sync:
            Enforces that the service will not return a response until the copy is complete.
        :keyword str source_authorization:
            Authenticate as a service principal using a client secret to access a source blob. Ensure "bearer " is
            the prefix of the source_authorization string. This option is only available when `incremental_copy` is
            set to False and `requires_sync` is set to True.

            .. versionadded:: 12.9.0

        :keyword source_token_intent:
            Required when source is Azure Storage Files and using `TokenCredential` for authentication.
            This is ignored for other forms of authentication.
            Specifies the intent for all requests when using `TokenCredential` authentication. Possible values are:

            backup - Specifies requests are intended for backup/admin type operations, meaning that all file/directory
                 ACLs are bypassed and full permissions are granted. User must also have required RBAC permission.

        :paramtype source_token_intent: Literal['backup']
        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the sync copied blob. An encryption
            scope can be created using the Management API and referenced here by name. If a default
            encryption scope has been defined at the container, this value will override it if the
            container-level scope is configured to allow overrides. Otherwise an error will be raised.

            .. versionadded:: 12.10.0

        :return: A dictionary of copy properties (etag, last_modified, copy_id, copy_status).
        :rtype: dict[str, Union[str, ~datetime.datetime]]

        .. admonition:: Example:

            .. literalinclude:: ../samples/blob_samples_common.py
                :start-after: [START copy_blob_from_url]
                :end-before: [END copy_blob_from_url]
                :language: python
                :dedent: 12
                :caption: Copy a blob from a URL.
        """
        options = _start_copy_from_url_options(
            source_url=source_url,
            metadata=metadata,
            incremental_copy=incremental_copy,
            **kwargs
        )
        try:
            if incremental_copy:
                return cast(Dict[str, Union[str, datetime]], self._client.page_blob.copy_incremental(**options))
            return cast(Dict[str, Union[str, datetime]], self._client.blob.start_copy_from_url(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def abort_copy(
        self, copy_id: Union[str, Dict[str, Any], BlobProperties],
        **kwargs: Any
    ) -> None:
        """Abort an ongoing copy operation.

        This will leave a destination blob with zero length and full metadata.
        This will raise an error if the copy operation has already ended.

        :param copy_id:
            The copy operation to abort. This can be either an ID string, or an
            instance of BlobProperties.
        :type copy_id: str or ~azure.storage.blob.BlobProperties
        :return: None
        :rtype: None

        .. admonition:: Example:

            .. literalinclude:: ../samples/blob_samples_common.py
                :start-after: [START abort_copy_blob_from_url]
                :end-before: [END abort_copy_blob_from_url]
                :language: python
                :dedent: 12
                :caption: Abort copying a blob from URL.
        """
        options = _abort_copy_options(copy_id, **kwargs)
        try:
            self._client.blob.abort_copy_from_url(**options)
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def acquire_lease(self, lease_duration: int =-1, lease_id: Optional[str] = None, **kwargs: Any) -> BlobLeaseClient:
        """Requests a new lease.

        If the blob does not have an active lease, the Blob
        Service creates a lease on the blob and returns a new lease.

        :param int lease_duration:
            Specifies the duration of the lease, in seconds, or negative one
            (-1) for a lease that never expires. A non-infinite lease can be
            between 15 and 60 seconds. A lease duration cannot be changed
            using renew or change. Default is -1 (infinite lease).
        :param str lease_id:
            Proposed lease ID, in a GUID string format. The Blob Service
            returns 400 (Invalid request) if the proposed lease ID is not
            in the correct format.
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: A BlobLeaseClient object.
        :rtype: ~azure.storage.blob.BlobLeaseClient

        .. admonition:: Example:

            .. literalinclude:: ../samples/blob_samples_common.py
                :start-after: [START acquire_lease_on_blob]
                :end-before: [END acquire_lease_on_blob]
                :language: python
                :dedent: 8
                :caption: Acquiring a lease on a blob.
        """
        lease = BlobLeaseClient(self, lease_id=lease_id)
        lease.acquire(lease_duration=lease_duration, **kwargs)
        return lease

    @distributed_trace
    def set_standard_blob_tier(self, standard_blob_tier: Union[str, "StandardBlobTier"], **kwargs: Any) -> None:
        """This operation sets the tier on a block blob.

        A block blob's tier determines Hot/Cool/Archive storage type.
        This operation does not update the blob's ETag.

        :param standard_blob_tier:
            Indicates the tier to be set on the blob. Options include 'Hot', 'Cool',
            'Archive'. The hot tier is optimized for storing data that is accessed
            frequently. The cool storage tier is optimized for storing data that
            is infrequently accessed and stored for at least a month. The archive
            tier is optimized for storing data that is rarely accessed and stored
            for at least six months with flexible latency requirements.
        :type standard_blob_tier: str or ~azure.storage.blob.StandardBlobTier
        :keyword ~azure.storage.blob.RehydratePriority rehydrate_priority:
            Indicates the priority with which to rehydrate an archived blob
        :keyword str version_id:
            The version id parameter is an opaque DateTime
            value that, when present, specifies the version of the blob to download.

            .. versionadded:: 12.4.0

            This keyword argument was introduced in API version '2019-12-12'.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :return: None
        :rtype: None
        """
        access_conditions = get_access_conditions(kwargs.pop('lease', None))
        mod_conditions = get_modify_conditions(kwargs)
        version_id = get_version_id(self.version_id, kwargs)
        if standard_blob_tier is None:
            raise ValueError("A StandardBlobTier must be specified")
        if self.snapshot and kwargs.get('version_id'):
            raise ValueError("Snapshot and version_id cannot be set at the same time")
        try:
            self._client.blob.set_tier(
                tier=standard_blob_tier,
                snapshot=self.snapshot,
                timeout=kwargs.pop('timeout', None),
                modified_access_conditions=mod_conditions,
                lease_access_conditions=access_conditions,
                version_id=version_id,
                **kwargs)
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def stage_block(
        self, block_id: str,
        data: Union[bytes, str, Iterable[AnyStr], IO[AnyStr]],
        length: Optional[int] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """Creates a new block to be committed as part of a blob.

        :param str block_id: A string value that identifies the block.
             The string should be less than or equal to 64 bytes in size.
             For a given blob, the block_id must be the same size for each block.
        :param data: The blob data.
        :type data: Union[bytes, str, Iterable[AnyStr], IO[AnyStr]]
        :param int length: Size of the block.
        :keyword bool validate_content:
            If true, calculates an MD5 hash for each chunk of the blob. The storage
            service checks the hash of the content that has arrived with the hash
            that was sent. This is primarily valuable for detecting bitflips on
            the wire if using http instead of https, as https (the default), will
            already validate. Note that this MD5 hash is not stored with the
            blob. Also note that if enabled, the memory-efficient upload algorithm
            will not be used because computing the MD5 hash requires buffering
            entire blocks, and doing so defeats the purpose of the memory-efficient algorithm.
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword str encoding:
            Defaults to UTF-8.
        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the service. An encryption
            scope can be created using the Management API and referenced here by name. If a default
            encryption scope has been defined at the container, this value will override it if the
            container-level scope is configured to allow overrides. Otherwise an error will be raised.

            .. versionadded:: 12.2.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Blob property dict.
        :rtype: dict[str, Any]
        """
        if self.require_encryption or (self.key_encryption_key is not None):
            raise ValueError(_ERROR_UNSUPPORTED_METHOD_FOR_ENCRYPTION)
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _stage_block_options(
            block_id=block_id,
            data=data,
            length=length,
            **kwargs)
        try:
            return cast(Dict[str, Any], self._client.block_blob.stage_block(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def stage_block_from_url(
        self, block_id: str,
        source_url: str,
        source_offset: Optional[int] = None,
        source_length: Optional[int] = None,
        source_content_md5: Optional[Union[bytes, bytearray]] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """Creates a new block to be committed as part of a blob where
        the contents are read from a URL.

        :param str block_id: A string value that identifies the block.
             The string should be less than or equal to 64 bytes in size.
             For a given blob, the block_id must be the same size for each block.
        :param str source_url: The URL.
        :param int source_offset:
            Start of byte range to use for the block.
            Must be set if source length is provided.
        :param int source_length: The size of the block in bytes.
        :param bytearray source_content_md5:
            Specify the md5 calculated for the range of
            bytes that must be read from the copy source.
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the service. An encryption
            scope can be created using the Management API and referenced here by name. If a default
            encryption scope has been defined at the container, this value will override it if the
            container-level scope is configured to allow overrides. Otherwise an error will be raised.

            .. versionadded:: 12.2.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :keyword str source_authorization:
            Authenticate as a service principal using a client secret to access a source blob. Ensure "bearer " is
            the prefix of the source_authorization string.
        :keyword source_token_intent:
            Required when source is Azure Storage Files and using `TokenCredential` for authentication.
            This is ignored for other forms of authentication.
            Specifies the intent for all requests when using `TokenCredential` authentication. Possible values are:

            backup - Specifies requests are intended for backup/admin type operations, meaning that all file/directory
                 ACLs are bypassed and full permissions are granted. User must also have required RBAC permission.

        :paramtype source_token_intent: Literal['backup']
        :return: Blob property dict.
        :rtype: dict[str, Any]
        """
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _stage_block_from_url_options(
            block_id=block_id,
            source_url=source_url,
            source_offset=source_offset,
            source_length=source_length,
            source_content_md5=source_content_md5,
            **kwargs
        )
        try:
            return cast(Dict[str, Any], self._client.block_blob.stage_block_from_url(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def get_block_list(
        self, block_list_type: str = "committed",
        **kwargs: Any
    ) -> Tuple[List[BlobBlock], List[BlobBlock]]:
        """The Get Block List operation retrieves the list of blocks that have
        been uploaded as part of a block blob.

        :param str block_list_type:
            Specifies whether to return the list of committed
            blocks, the list of uncommitted blocks, or both lists together.
            Possible values include: 'committed', 'uncommitted', 'all'
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on destination blob with a matching value.

            .. versionadded:: 12.4.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: A tuple of two lists - committed and uncommitted blocks
        :rtype: Tuple[List[BlobBlock], List[BlobBlock]]
        """
        access_conditions = get_access_conditions(kwargs.pop('lease', None))
        mod_conditions = get_modify_conditions(kwargs)
        try:
            blocks = self._client.block_blob.get_block_list(
                list_type=block_list_type,
                snapshot=self.snapshot,
                timeout=kwargs.pop('timeout', None),
                lease_access_conditions=access_conditions,
                modified_access_conditions=mod_conditions,
                **kwargs)
        except HttpResponseError as error:
            process_storage_error(error)
        return _get_block_list_result(blocks)

    @distributed_trace
    def commit_block_list(
        self, block_list: List[BlobBlock],
        content_settings: Optional["ContentSettings"] = None,
        metadata: Optional[Dict[str, str]] = None,
        **kwargs: Any
    ) -> Dict[str, Union[str, datetime]]:
        """The Commit Block List operation writes a blob by specifying the list of
        block IDs that make up the blob.

        :param list block_list:
            List of Blockblobs.
        :param ~azure.storage.blob.ContentSettings content_settings:
            ContentSettings object used to set blob properties. Used to set content type, encoding,
            language, disposition, md5, and cache control.
        :param metadata:
            Name-value pairs associated with the blob as metadata.
        :type metadata: dict[str, str]
        :keyword tags:
            Name-value pairs associated with the blob as tag. Tags are case-sensitive.
            The tag set may contain at most 10 tags.  Tag keys must be between 1 and 128 characters,
            and tag values must be between 0 and 256 characters.
            Valid tag key and value characters include: lowercase and uppercase letters, digits (0-9),
            space (' '), plus (+), minus (-), period (.), solidus (/), colon (:), equals (=), underscore (_)

            .. versionadded:: 12.4.0

        :paramtype tags: dict(str, str)
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~azure.storage.blob.ImmutabilityPolicy immutability_policy:
            Specifies the immutability policy of a blob, blob snapshot or blob version.

            .. versionadded:: 12.10.0
                This was introduced in API version '2020-10-02'.

        :keyword bool legal_hold:
            Specified if a legal hold should be set on the blob.

            .. versionadded:: 12.10.0
                This was introduced in API version '2020-10-02'.

        :keyword bool validate_content:
            If true, calculates an MD5 hash of the page content. The storage
            service checks the hash of the content that has arrived
            with the hash that was sent. This is primarily valuable for detecting
            bitflips on the wire if using http instead of https, as https (the default),
            will already validate. Note that this MD5 hash is not stored with the
            blob.
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on destination blob with a matching value.

            .. versionadded:: 12.4.0

        :keyword ~azure.storage.blob.StandardBlobTier standard_blob_tier:
            A standard blob tier value to set the blob to. For this version of the library,
            this is only applicable to block blobs on standard storage accounts.
        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the service. An encryption
            scope can be created using the Management API and referenced here by name. If a default
            encryption scope has been defined at the container, this value will override it if the
            container-level scope is configured to allow overrides. Otherwise an error will be raised.

            .. versionadded:: 12.2.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Blob-updated property dict (Etag and last modified).
        :rtype: dict(str, Any)
        """
        if self.require_encryption or (self.key_encryption_key is not None):
            raise ValueError(_ERROR_UNSUPPORTED_METHOD_FOR_ENCRYPTION)
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _commit_block_list_options(
            block_list=block_list,
            content_settings=content_settings,
            metadata=metadata,
            **kwargs)
        try:
            return cast(Dict[str, Any], self._client.block_blob.commit_block_list(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def set_premium_page_blob_tier(self, premium_page_blob_tier: "PremiumPageBlobTier", **kwargs: Any) -> None:
        """Sets the page blob tiers on the blob. This API is only supported for page blobs on premium accounts.

        :param premium_page_blob_tier:
            A page blob tier value to set the blob to. The tier correlates to the size of the
            blob and number of allowed IOPS. This is only applicable to page blobs on
            premium storage accounts.
        :type premium_page_blob_tier: ~azure.storage.blob.PremiumPageBlobTier
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :return: None
        :rtype: None
        """
        access_conditions = get_access_conditions(kwargs.pop('lease', None))
        mod_conditions = get_modify_conditions(kwargs)
        if premium_page_blob_tier is None:
            raise ValueError("A PremiumPageBlobTier must be specified")
        try:
            self._client.blob.set_tier(
                tier=premium_page_blob_tier,
                timeout=kwargs.pop('timeout', None),
                lease_access_conditions=access_conditions,
                modified_access_conditions=mod_conditions,
                **kwargs)
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def set_blob_tags(self, tags: Optional[Dict[str, str]] = None, **kwargs: Any) -> Dict[str, Any]:
        """The Set Tags operation enables users to set tags on a blob or specific blob version, but not snapshot.
            Each call to this operation replaces all existing tags attached to the blob. To remove all
            tags from the blob, call this operation with no tags set.

        .. versionadded:: 12.4.0
            This operation was introduced in API version '2019-12-12'.

        :param tags:
            Name-value pairs associated with the blob as tag. Tags are case-sensitive.
            The tag set may contain at most 10 tags.  Tag keys must be between 1 and 128 characters,
            and tag values must be between 0 and 256 characters.
            Valid tag key and value characters include: lowercase and uppercase letters, digits (0-9),
            space (' '), plus (+), minus (-), period (.), solidus (/), colon (:), equals (=), underscore (_)
        :type tags: dict(str, str)
        :keyword str version_id:
            The version id parameter is an opaque DateTime
            value that, when present, specifies the version of the blob to add tags to.
        :keyword bool validate_content:
            If true, calculates an MD5 hash of the tags content. The storage
            service checks the hash of the content that has arrived
            with the hash that was sent. This is primarily valuable for detecting
            bitflips on the wire if using http instead of https, as https (the default),
            will already validate. Note that this MD5 hash is not stored with the
            blob.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on destination blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Blob-updated property dict (Etag and last modified)
        :rtype: Dict[str, Any]
        """
        version_id = get_version_id(self.version_id, kwargs)
        options = _set_blob_tags_options(version_id=version_id, tags=tags, **kwargs)
        try:
            return cast(Dict[str, Any], self._client.blob.set_tags(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def get_blob_tags(self, **kwargs: Any) -> Dict[str, str]:
        """The Get Tags operation enables users to get tags on a blob or specific blob version, or snapshot.

        .. versionadded:: 12.4.0
            This operation was introduced in API version '2019-12-12'.

        :keyword Optional[str] version_id:
            The version id parameter is an opaque DateTime
            value that, when present, specifies the version of the blob to add tags to.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on destination blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Key value pairs of blob tags.
        :rtype: Dict[str, str]
        """
        version_id = get_version_id(self.version_id, kwargs)
        options = _get_blob_tags_options(version_id=version_id, snapshot=self.snapshot, **kwargs)
        try:
            _, tags = self._client.blob.get_tags(**options)
            return cast(Dict[str, str], parse_tags(tags))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def get_page_ranges(
        self, offset: Optional[int] = None,
        length: Optional[int] = None,
        previous_snapshot_diff: Optional[Union[str, Dict[str, Any]]] = None,
        **kwargs: Any
    ) -> Tuple[List[Dict[str, int]], List[Dict[str, int]]]:
        """DEPRECATED: Returns the list of valid page ranges for a Page Blob or snapshot
        of a page blob.

        :param int offset:
            Start of byte range to use for getting valid page ranges.
            If no length is given, all bytes after the offset will be searched.
            Pages must be aligned with 512-byte boundaries, the start offset
            must be a modulus of 512 and the length must be a modulus of
            512.
        :param int length:
            Number of bytes to use for getting valid page ranges.
            If length is given, offset must be provided.
            This range will return valid page ranges from the offset start up to
            the specified length.
            Pages must be aligned with 512-byte boundaries, the start offset
            must be a modulus of 512 and the length must be a modulus of
            512.
        :param str previous_snapshot_diff:
            The snapshot diff parameter that contains an opaque DateTime value that
            specifies a previous blob snapshot to be compared
            against a more recent snapshot or the current blob.
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return:
            A tuple of two lists of page ranges as dictionaries with 'start' and 'end' keys.
            The first element are filled page ranges, the 2nd element is cleared page ranges.
        :rtype: tuple(list(dict(str, str), list(dict(str, str))
        """
        warnings.warn(
            "get_page_ranges is deprecated, use list_page_ranges instead",
            DeprecationWarning
        )

        options = _get_page_ranges_options(
            snapshot=self.snapshot,
            offset=offset,
            length=length,
            previous_snapshot_diff=previous_snapshot_diff,
            **kwargs)
        try:
            if previous_snapshot_diff:
                ranges = self._client.page_blob.get_page_ranges_diff(**options)
            else:
                ranges = self._client.page_blob.get_page_ranges(**options)
        except HttpResponseError as error:
            process_storage_error(error)
        return get_page_ranges_result(ranges)

    @distributed_trace
    def list_page_ranges(
        self,
        *,
        offset: Optional[int] = None,
        length: Optional[int] = None,
        previous_snapshot: Optional[Union[str, Dict[str, Any]]] = None,
        **kwargs: Any
    ) -> ItemPaged[PageRange]:
        """Returns the list of valid page ranges for a Page Blob or snapshot
        of a page blob. If `previous_snapshot` is specified, the result will be
        a diff of changes between the target blob and the previous snapshot.

        :keyword int offset:
            Start of byte range to use for getting valid page ranges.
            If no length is given, all bytes after the offset will be searched.
            Pages must be aligned with 512-byte boundaries, the start offset
            must be a modulus of 512 and the length must be a modulus of
            512.
        :keyword int length:
            Number of bytes to use for getting valid page ranges.
            If length is given, offset must be provided.
            This range will return valid page ranges from the offset start up to
            the specified length.
            Pages must be aligned with 512-byte boundaries, the start offset
            must be a modulus of 512 and the length must be a modulus of
            512.
        :keyword previous_snapshot:
            A snapshot value that specifies that the response will contain only pages that were changed
            between target blob and previous snapshot. Changed pages include both updated and cleared
            pages. The target blob may be a snapshot, as long as the snapshot specified by `previous_snapshot`
            is the older of the two.
        :paramtype previous_snapshot: str or Dict[str, Any]
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword int results_per_page:
            The maximum number of page ranges to retrieve per API call.
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: An iterable (auto-paging) of PageRange.
        :rtype: ~azure.core.paging.ItemPaged[~azure.storage.blob.PageRange]
        """
        results_per_page = kwargs.pop('results_per_page', None)
        options = _get_page_ranges_options(
            snapshot=self.snapshot,
            offset=offset,
            length=length,
            previous_snapshot_diff=previous_snapshot,
            **kwargs)

        if previous_snapshot:
            command = partial(
                self._client.page_blob.get_page_ranges_diff,
                **options)
        else:
            command = partial(
                self._client.page_blob.get_page_ranges,
                **options)
        return ItemPaged(
            command, results_per_page=results_per_page,
            page_iterator_class=PageRangePaged)

    @distributed_trace
    def get_page_range_diff_for_managed_disk(
        self, previous_snapshot_url: str,
        offset: Optional[int] = None,
        length:Optional[int] = None,
        **kwargs: Any
    ) -> Tuple[List[Dict[str, int]], List[Dict[str, int]]]:
        """Returns the list of valid page ranges for a managed disk or snapshot.

        .. note::
            This operation is only available for managed disk accounts.

        .. versionadded:: 12.2.0
            This operation was introduced in API version '2019-07-07'.

        :param str previous_snapshot_url:
            Specifies the URL of a previous snapshot of the managed disk.
            The response will only contain pages that were changed between the target blob and
            its previous snapshot.
        :param int offset:
            Start of byte range to use for getting valid page ranges.
            If no length is given, all bytes after the offset will be searched.
            Pages must be aligned with 512-byte boundaries, the start offset
            must be a modulus of 512 and the length must be a modulus of
            512.
        :param int length:
            Number of bytes to use for getting valid page ranges.
            If length is given, offset must be provided.
            This range will return valid page ranges from the offset start up to
            the specified length.
            Pages must be aligned with 512-byte boundaries, the start offset
            must be a modulus of 512 and the length must be a modulus of
            512.
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return:
            A tuple of two lists of page ranges as dictionaries with 'start' and 'end' keys.
            The first element are filled page ranges, the 2nd element is cleared page ranges.
        :rtype: tuple(list(dict(str, str), list(dict(str, str))
        """
        options = _get_page_ranges_options(
            snapshot=self.snapshot,
            offset=offset,
            length=length,
            prev_snapshot_url=previous_snapshot_url,
            **kwargs)
        try:
            ranges = self._client.page_blob.get_page_ranges_diff(**options)
        except HttpResponseError as error:
            process_storage_error(error)
        return get_page_ranges_result(ranges)

    @distributed_trace
    def set_sequence_number(
        self, sequence_number_action: Union[str, "SequenceNumberAction"],
        sequence_number: Optional[str] = None,
        **kwargs: Any
    ) -> Dict[str, Union[str, datetime]]:
        """Sets the blob sequence number.

        :param str sequence_number_action:
            This property indicates how the service should modify the blob's sequence
            number. See :class:`~azure.storage.blob.SequenceNumberAction` for more information.
        :param str sequence_number:
            This property sets the blob's sequence number. The sequence number is a
            user-controlled property that you can use to track requests and manage
            concurrency issues.
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Blob-updated property dict (Etag and last modified).
        :rtype: dict(str, Any)
        """
        options = _set_sequence_number_options(sequence_number_action, sequence_number=sequence_number, **kwargs)
        try:
            return cast(Dict[str, Any], self._client.page_blob.update_sequence_number(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def resize_blob(self, size: int, **kwargs: Any) -> Dict[str, Union[str, datetime]]:
        """Resizes a page blob to the specified size.

        If the specified value is less than the current size of the blob,
        then all pages above the specified value are cleared.

        :param int size:
            Size used to resize blob. Maximum size for a page blob is up to 1 TB.
            The page blob size must be aligned to a 512-byte boundary.
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword ~azure.storage.blob.PremiumPageBlobTier premium_page_blob_tier:
            A page blob tier value to set the blob to. The tier correlates to the size of the
            blob and number of allowed IOPS. This is only applicable to page blobs on
            premium storage accounts.
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Blob-updated property dict (Etag and last modified).
        :rtype: dict(str, Any)
        """
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _resize_blob_options(size=size, **kwargs)
        try:
            return cast(Dict[str, Any], self._client.page_blob.resize(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def upload_page(
        self, page: bytes,
        offset: int,
        length: int,
        **kwargs: Any
    ) -> Dict[str, Union[str, datetime]]:
        """The Upload Pages operation writes a range of pages to a page blob.

        :param bytes page:
            Content of the page.
        :param int offset:
            Start of byte range to use for writing to a section of the blob.
            Pages must be aligned with 512-byte boundaries, the start offset
            must be a modulus of 512 and the length  must be a modulus of
            512.
        :param int length:
            Number of bytes to use for writing to a section of the blob.
            Pages must be aligned with 512-byte boundaries, the start offset
            must be a modulus of 512 and the length must be a modulus of
            512.
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword bool validate_content:
            If true, calculates an MD5 hash of the page content. The storage
            service checks the hash of the content that has arrived
            with the hash that was sent. This is primarily valuable for detecting
            bitflips on the wire if using http instead of https, as https (the default),
            will already validate. Note that this MD5 hash is not stored with the
            blob.
        :keyword int if_sequence_number_lte:
            If the blob's sequence number is less than or equal to
            the specified value, the request proceeds; otherwise it fails.
        :keyword int if_sequence_number_lt:
            If the blob's sequence number is less than the specified
            value, the request proceeds; otherwise it fails.
        :keyword int if_sequence_number_eq:
            If the blob's sequence number is equal to the specified
            value, the request proceeds; otherwise it fails.
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the service. An encryption
            scope can be created using the Management API and referenced here by name. If a default
            encryption scope has been defined at the container, this value will override it if the
            container-level scope is configured to allow overrides. Otherwise an error will be raised.

            .. versionadded:: 12.2.0

        :keyword str encoding:
            Defaults to UTF-8.
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Blob-updated property dict (Etag and last modified).
        :rtype: dict(str, Any)
        """
        if self.require_encryption or (self.key_encryption_key is not None):
            raise ValueError(_ERROR_UNSUPPORTED_METHOD_FOR_ENCRYPTION)
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _upload_page_options(
            page=page,
            offset=offset,
            length=length,
            **kwargs)
        try:
            return cast(Dict[str, Any], self._client.page_blob.upload_pages(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def upload_pages_from_url(
        self, source_url: str,
        offset: int,
        length: int,
        source_offset: int,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """
        The Upload Pages operation writes a range of pages to a page blob where
        the contents are read from a URL.

        :param str source_url:
            The URL of the source data. It can point to any Azure Blob or File, that is either public or has a
            shared access signature attached.
        :param int offset:
            Start of byte range to use for writing to a section of the blob.
            Pages must be aligned with 512-byte boundaries, the start offset
            must be a modulus of 512 and the length  must be a modulus of
            512.
        :param int length:
            Number of bytes to use for writing to a section of the blob.
            Pages must be aligned with 512-byte boundaries, the start offset
            must be a modulus of 512 and the length must be a modulus of
            512.
        :param int source_offset:
            This indicates the start of the range of bytes(inclusive) that has to be taken from the copy source.
            The service will read the same number of bytes as the destination range (length-offset).
        :keyword bytes source_content_md5:
            If given, the service will calculate the MD5 hash of the block content and compare against this value.
        :keyword ~datetime.datetime source_if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the source resource has been modified since the specified time.
        :keyword ~datetime.datetime source_if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the source resource has not been modified since the specified date/time.
        :keyword str source_etag:
            The source ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions source_match_condition:
            The source match condition to use upon the etag.
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword int if_sequence_number_lte:
            If the blob's sequence number is less than or equal to
            the specified value, the request proceeds; otherwise it fails.
        :keyword int if_sequence_number_lt:
            If the blob's sequence number is less than the specified
            value, the request proceeds; otherwise it fails.
        :keyword int if_sequence_number_eq:
            If the blob's sequence number is equal to the specified
            value, the request proceeds; otherwise it fails.
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            The destination ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The destination match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the service. An encryption
            scope can be created using the Management API and referenced here by name. If a default
            encryption scope has been defined at the container, this value will override it if the
            container-level scope is configured to allow overrides. Otherwise an error will be raised.

            .. versionadded:: 12.2.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :keyword str source_authorization:
            Authenticate as a service principal using a client secret to access a source blob. Ensure "bearer " is
            the prefix of the source_authorization string.
        :keyword source_token_intent:
            Required when source is Azure Storage Files and using `TokenCredential` for authentication.
            This is ignored for other forms of authentication.
            Specifies the intent for all requests when using `TokenCredential` authentication. Possible values are:

            backup - Specifies requests are intended for backup/admin type operations, meaning that all file/directory
                 ACLs are bypassed and full permissions are granted. User must also have required RBAC permission.

        :paramtype source_token_intent: Literal['backup']
        :return: Response after uploading pages from specified URL.
        :rtype: Dict[str, Any]
        """
        if self.require_encryption or (self.key_encryption_key is not None):
            raise ValueError(_ERROR_UNSUPPORTED_METHOD_FOR_ENCRYPTION)
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _upload_pages_from_url_options(
            source_url=source_url,
            offset=offset,
            length=length,
            source_offset=source_offset,
            **kwargs
        )
        try:
            return cast(Dict[str, Any], self._client.page_blob.upload_pages_from_url(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def clear_page(self, offset: int, length: int, **kwargs: Any) -> Dict[str, Union[str, datetime]]:
        """Clears a range of pages.

        :param int offset:
            Start of byte range to use for writing to a section of the blob.
            Pages must be aligned with 512-byte boundaries, the start offset
            must be a modulus of 512 and the length must be a modulus of
            512.
        :param int length:
            Number of bytes to use for writing to a section of the blob.
            Pages must be aligned with 512-byte boundaries, the start offset
            must be a modulus of 512 and the length must be a modulus of
            512.
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword int if_sequence_number_lte:
            If the blob's sequence number is less than or equal to
            the specified value, the request proceeds; otherwise it fails.
        :keyword int if_sequence_number_lt:
            If the blob's sequence number is less than the specified
            value, the request proceeds; otherwise it fails.
        :keyword int if_sequence_number_eq:
            If the blob's sequence number is equal to the specified
            value, the request proceeds; otherwise it fails.
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Blob-updated property dict (Etag and last modified).
        :rtype: dict(str, Any)
        """
        if self.require_encryption or (self.key_encryption_key is not None):
            raise ValueError(_ERROR_UNSUPPORTED_METHOD_FOR_ENCRYPTION)
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _clear_page_options(
            offset=offset,
            length=length,
            **kwargs
        )
        try:
            return cast(Dict[str, Any], self._client.page_blob.clear_pages(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def append_block(
        self, data: Union[bytes, str, Iterable[AnyStr], IO[AnyStr]],
        length: Optional[int] = None,
        **kwargs: Any
    ) -> Dict[str, Union[str, datetime, int]]:
        """Commits a new block of data to the end of the existing append blob.

        :param data:
            Content of the block. This can be bytes, text, an iterable or a file-like object.
        :type data: bytes or str or Iterable
        :param int length:
            Size of the block in bytes.
        :keyword bool validate_content:
            If true, calculates an MD5 hash of the block content. The storage
            service checks the hash of the content that has arrived
            with the hash that was sent. This is primarily valuable for detecting
            bitflips on the wire if using http instead of https, as https (the default),
            will already validate. Note that this MD5 hash is not stored with the
            blob.
        :keyword int maxsize_condition:
            Optional conditional header. The max length in bytes permitted for
            the append blob. If the Append Block operation would cause the blob
            to exceed that limit or if the blob size is already greater than the
            value specified in this header, the request will fail with
            MaxBlobSizeConditionNotMet error (HTTP status code 412 - Precondition Failed).
        :keyword int appendpos_condition:
            Optional conditional header, used only for the Append Block operation.
            A number indicating the byte offset to compare. Append Block will
            succeed only if the append position is equal to this number. If it
            is not, the request will fail with the AppendPositionConditionNotMet error
            (HTTP status code 412 - Precondition Failed).
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword str encoding:
            Defaults to UTF-8.
        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the service. An encryption
            scope can be created using the Management API and referenced here by name. If a default
            encryption scope has been defined at the container, this value will override it if the
            container-level scope is configured to allow overrides. Otherwise an error will be raised.

            .. versionadded:: 12.2.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Blob-updated property dict (Etag, last modified, append offset, committed block count).
        :rtype: dict(str, Any)
        """
        if self.require_encryption or (self.key_encryption_key is not None):
            raise ValueError(_ERROR_UNSUPPORTED_METHOD_FOR_ENCRYPTION)
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _append_block_options(
            data=data,
            length=length,
            **kwargs
        )
        try:
            return cast(Dict[str, Any], self._client.append_blob.append_block(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def append_block_from_url(
        self, copy_source_url: str,
        source_offset: Optional[int] = None,
        source_length: Optional[int] = None,
        **kwargs: Any
    ) -> Dict[str, Union[str, datetime, int]]:
        """
        Creates a new block to be committed as part of a blob, where the contents are read from a source url.

        :param str copy_source_url:
            The URL of the source data. It can point to any Azure Blob or File, that is either public or has a
            shared access signature attached.
        :param int source_offset:
            This indicates the start of the range of bytes (inclusive) that has to be taken from the copy source.
        :param int source_length:
            This indicates the end of the range of bytes that has to be taken from the copy source.
        :keyword bytearray source_content_md5:
            If given, the service will calculate the MD5 hash of the block content and compare against this value.
        :keyword int maxsize_condition:
            Optional conditional header. The max length in bytes permitted for
            the append blob. If the Append Block operation would cause the blob
            to exceed that limit or if the blob size is already greater than the
            value specified in this header, the request will fail with
            MaxBlobSizeConditionNotMet error (HTTP status code 412 - Precondition Failed).
        :keyword int appendpos_condition:
            Optional conditional header, used only for the Append Block operation.
            A number indicating the byte offset to compare. Append Block will
            succeed only if the append position is equal to this number. If it
            is not, the request will fail with the
            AppendPositionConditionNotMet error
            (HTTP status code 412 - Precondition Failed).
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            The destination ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The destination match condition to use upon the etag.
        :keyword str if_tags_match_condition:
            Specify a SQL where clause on blob tags to operate only on blob with a matching value.
            eg. ``\"\\\"tagname\\\"='my tag'\"``

            .. versionadded:: 12.4.0

        :keyword ~datetime.datetime source_if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the source resource has been modified since the specified time.
        :keyword ~datetime.datetime source_if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the source resource has not been modified since the specified date/time.
        :keyword str source_etag:
            The source ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions source_match_condition:
            The source match condition to use upon the etag.
        :keyword ~azure.storage.blob.CustomerProvidedEncryptionKey cpk:
            Encrypts the data on the service-side with the given key.
            Use of customer-provided keys must be done over HTTPS.
            As the encryption key itself is provided in the request,
            a secure connection must be established to transfer the key.
        :keyword str encryption_scope:
            A predefined encryption scope used to encrypt the data on the service. An encryption
            scope can be created using the Management API and referenced here by name. If a default
            encryption scope has been defined at the container, this value will override it if the
            container-level scope is configured to allow overrides. Otherwise an error will be raised.

            .. versionadded:: 12.2.0

        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :keyword str source_authorization:
            Authenticate as a service principal using a client secret to access a source blob. Ensure "bearer " is
            the prefix of the source_authorization string.
        :keyword source_token_intent:
            Required when source is Azure Storage Files and using `TokenCredential` for authentication.
            This is ignored for other forms of authentication.
            Specifies the intent for all requests when using `TokenCredential` authentication. Possible values are:

            backup - Specifies requests are intended for backup/admin type operations, meaning that all file/directory
                 ACLs are bypassed and full permissions are granted. User must also have required RBAC permission.

        :paramtype source_token_intent: Literal['backup']
        :return: Result after appending a new block.
        :rtype: Dict[str, Union[str, datetime, int]]
        """
        if self.require_encryption or (self.key_encryption_key is not None):
            raise ValueError(_ERROR_UNSUPPORTED_METHOD_FOR_ENCRYPTION)
        if kwargs.get('cpk') and self.scheme.lower() != 'https':
            raise ValueError("Customer provided encryption key must be used over HTTPS.")
        options = _append_block_from_url_options(
            copy_source_url=copy_source_url,
            source_offset=source_offset,
            source_length=source_length,
            **kwargs
        )
        try:
            return cast(Dict[str, Union[str, datetime, int]],
                        self._client.append_blob.append_block_from_url(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def seal_append_blob(self, **kwargs: Any) -> Dict[str, Union[str, datetime, int]]:
        """The Seal operation seals the Append Blob to make it read-only.

            .. versionadded:: 12.4.0

        :keyword int appendpos_condition:
            Optional conditional header, used only for the Append Block operation.
            A number indicating the byte offset to compare. Append Block will
            succeed only if the append position is equal to this number. If it
            is not, the request will fail with the AppendPositionConditionNotMet error
            (HTTP status code 412 - Precondition Failed).
        :keyword lease:
            Required if the blob has an active lease. Value can be a BlobLeaseClient object
            or the lease ID as a string.
        :paramtype lease: ~azure.storage.blob.BlobLeaseClient or str
        :keyword ~datetime.datetime if_modified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only
            if the resource has been modified since the specified time.
        :keyword ~datetime.datetime if_unmodified_since:
            A DateTime value. Azure expects the date value passed in to be UTC.
            If timezone is included, any non-UTC datetimes will be converted to UTC.
            If a date is passed in without timezone info, it is assumed to be UTC.
            Specify this header to perform the operation only if
            the resource has not been modified since the specified date/time.
        :keyword str etag:
            An ETag value, or the wildcard character (*). Used to check if the resource has changed,
            and act according to the condition specified by the `match_condition` parameter.
        :keyword ~azure.core.MatchConditions match_condition:
            The match condition to use upon the etag.
        :keyword int timeout:
            Sets the server-side timeout for the operation in seconds. For more details see
            https://learn.microsoft.com/rest/api/storageservices/setting-timeouts-for-blob-service-operations.
            This value is not tracked or validated on the client. To configure client-side network timesouts
            see `here <https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-blob
            #other-client--per-operation-configuration>`__.
        :return: Blob-updated property dict (Etag, last modified, append offset, committed block count).
        :rtype: dict(str, Any)
        """
        if self.require_encryption or (self.key_encryption_key is not None):
            raise ValueError(_ERROR_UNSUPPORTED_METHOD_FOR_ENCRYPTION)
        options = _seal_append_blob_options(**kwargs)
        try:
            return cast(Dict[str, Any], self._client.append_blob.seal(**options))
        except HttpResponseError as error:
            process_storage_error(error)

    @distributed_trace
    def _get_container_client(self) -> "ContainerClient":
        """Get a client to interact with the blob's parent container.

        The container need not already exist. Defaults to current blob's credentials.

        :return: A ContainerClient.
        :rtype: ~azure.storage.blob.ContainerClient

        .. admonition:: Example:

            .. literalinclude:: ../samples/blob_samples_containers.py
                :start-after: [START get_container_client_from_blob_client]
                :end-before: [END get_container_client_from_blob_client]
                :language: python
                :dedent: 8
                :caption: Get container client from blob object.
        """
        from ._container_client import ContainerClient
        if not isinstance(self._pipeline._transport, TransportWrapper): # pylint: disable = protected-access
            _pipeline = Pipeline(
                transport=TransportWrapper(self._pipeline._transport), # pylint: disable = protected-access
                policies=self._pipeline._impl_policies # pylint: disable = protected-access
            )
        else:
            _pipeline = self._pipeline
        return ContainerClient(
            f"{self.scheme}://{self.primary_hostname}", container_name=self.container_name,
            credential=self._raw_credential, api_version=self.api_version, _configuration=self._config,
            _pipeline=_pipeline, _location_mode=self._location_mode, _hosts=self._hosts,
            require_encryption=self.require_encryption, encryption_version=self.encryption_version,
            key_encryption_key=self.key_encryption_key, key_resolver_function=self.key_resolver_function)
