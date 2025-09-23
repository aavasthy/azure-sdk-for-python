import os
import uuid
import asyncio
import unittest
import pytest

from azure.identity import ManagedIdentityCredential
from azure.cosmos import exceptions
from azure.cosmos.aio import CosmosClient


def get_test_item(i: int, pk: str, unique_id: str):
    return {
        "id": f"Item_{unique_id}_{i}",
        "pk": pk,
        "test_object": True,
        "lastName": "Smith",
        "value": f"sample-{i}"
    }


async def _create_one(host: str, database_id: str, container_id: str, pk: str, i: int, credential) -> str:
    client = CosmosClient(host, credential)
    try:
        db = client.get_database_client(database_id)
        container = db.get_container_client(container_id)
        uid = str(uuid.uuid4())
        item = get_test_item(i, pk, uid)
        created = await container.create_item(item)
        return created["id"]
    finally:
        await client.close()


@pytest.mark.cosmosEmulator
class TestAADAsync(unittest.IsolatedAsyncioTestCase):
    async def test_aad_scope_override_async(self):
        os.environ["AZURE_COSMOS_AAD_SCOPE_OVERRIDE"] = "https://cosmos.azure.com/.default"

        host1 = "https://b9a82029-f577-44d8-99ec-939209dd0c1c.zb9.msit-sql.cosmos.fabric.microsoft.com:443/"
        host2 = "https://aadaudiencetest.documents.azure.com:443/"
        database_id = "TestMsFabricDb"
        container_id = "TestMsFabricContainerNew"
        partition_key_value = "partition1"

        msi_client_id = os.getenv("AZURE_CLIENT_ID")  # optional
        credential = ManagedIdentityCredential(client_id=msi_client_id)


        token1 = await asyncio.to_thread(credential.get_token, "https://cosmos.azure.com/.default")
        print(token1)
        token2 = await asyncio.to_thread(credential.get_token, "https://cosmos.azure.com/.default")
        print(token2)
        token3 = await asyncio.to_thread(credential.get_token, "https://cosmos.azure.com/.default")
        print(token3)

        try:
            tasks = [
                _create_one(host1 if i < 5 else host2, database_id, container_id, partition_key_value, i, credential)
                for i in range(10)
            ]
            created_ids = await asyncio.gather(*tasks)
            assert len(created_ids) == 10
        except exceptions.CosmosHttpResponseError as ex:
            pytest.fail(f"Create failed: {ex}")


if __name__ == "__main__":
    unittest.main()
