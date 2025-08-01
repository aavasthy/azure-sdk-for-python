# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# Code generated by Microsoft (R) AutoRest Code Generator.
# Changes may cause incorrect behavior and will be lost if the code is regenerated.
# --------------------------------------------------------------------------
import pytest
from azure.mgmt.search import SearchManagementClient

from devtools_testutils import AzureMgmtRecordedTestCase, RandomNameResourceGroupPreparer, recorded_by_proxy

AZURE_LOCATION = "eastus"


@pytest.mark.skip("you may need to update the auto-generated test case before run it")
class TestSearchManagementAdminKeysOperations(AzureMgmtRecordedTestCase):
    def setup_method(self, method):
        self.client = self.create_mgmt_client(SearchManagementClient)

    @RandomNameResourceGroupPreparer(location=AZURE_LOCATION)
    @recorded_by_proxy
    def test_admin_keys_get(self, resource_group):
        response = self.client.admin_keys.get(
            resource_group_name=resource_group.name,
            search_service_name="str",
            api_version="2025-05-01",
        )

        # please add some check logic here by yourself
        # ...

    @RandomNameResourceGroupPreparer(location=AZURE_LOCATION)
    @recorded_by_proxy
    def test_admin_keys_regenerate(self, resource_group):
        response = self.client.admin_keys.regenerate(
            resource_group_name=resource_group.name,
            search_service_name="str",
            key_kind="str",
            api_version="2025-05-01",
        )

        # please add some check logic here by yourself
        # ...
