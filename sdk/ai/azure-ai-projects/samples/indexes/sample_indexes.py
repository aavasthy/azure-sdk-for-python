# ------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------

"""
DESCRIPTION:
    Given an AIProjectClient, this sample demonstrates how to use the synchronous
    `.indexes` methods to upload a file, create Indexes that reference those files,
    list Indexes and delete Indexes.

USAGE:
    python sample_indexes.py

    Before running the sample:

    pip install azure-ai-projects azure-identity

    Set these environment variables with your own values:
    1) PROJECT_ENDPOINT - Required. The Azure AI Project endpoint, as found in the overview page of your
       Azure AI Foundry project.
    2) INDEX_NAME - Optional. The name of the Index to create and use in this sample.
    3) INDEX_VERSION - Optional. The version of the Index to create and use in this sample.
    4) AI_SEARCH_CONNECTION_NAME - Optional. The name of an existing AI Search connection to use in this sample.
    5) AI_SEARCH_INDEX_NAME - Optional. The name of the AI Search index to use in this sample.
"""

import os
from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import AzureAISearchIndex

endpoint = os.environ["PROJECT_ENDPOINT"]
index_name = os.environ.get("INDEX_NAME", "index-test")
index_version = os.environ.get("INDEX_VERSION", "1.0")
ai_search_connection_name = os.environ.get("AI_SEARCH_CONNECTION_NAME", "my-ai-search-connection-name")
ai_search_index_name = os.environ.get("AI_SEARCH_INDEX_NAME", "my-ai-search-index-name")

with DefaultAzureCredential(exclude_interactive_browser_credential=False) as credential:

    with AIProjectClient(endpoint=endpoint, credential=credential) as project_client:

        # [START indexes_sample]
        print(
            f"Create Index `{index_name}` with version `{index_version}`, referencing an existing AI Search resource:"
        )
        index = project_client.indexes.create_or_update(
            name=index_name,
            version=index_version,
            index=AzureAISearchIndex(connection_name=ai_search_connection_name, index_name=ai_search_index_name),
        )
        print(index)

        print(f"Get Index `{index_name}` version `{index_version}`:")
        index = project_client.indexes.get(name=index_name, version=index_version)
        print(index)

        print("List latest versions of all Indexes:")
        for index in project_client.indexes.list():
            print(index)

        print(f"Listing all versions of the Index named `{index_name}`:")
        for index in project_client.indexes.list_versions(name=index_name):
            print(index)

        print(f"Delete Index `{index_name}` version `{index_version}`:")
        project_client.indexes.delete(name=index_name, version=index_version)
        # [END indexes_sample]
