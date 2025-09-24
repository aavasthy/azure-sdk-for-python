import os
import uuid
import asyncio
import unittest
import pytest
import jwt   # pip install pyjwt
import datetime

from azure.identity.aio import DefaultAzureCredential
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


async def log_token(cred, scope: str, label: str):
    token = await cred.get_token(scope)
    decoded = jwt.decode(token.token, options={"verify_signature": False})
    exp_time = datetime.datetime.utcfromtimestamp(token.expires_on)
    print(f"[{label}] oid={decoded.get('oid')}, jti={decoded.get('jti')}, exp={exp_time}")
    return token


@pytest.mark.cosmosEmulator
class TestAADAsync(unittest.IsolatedAsyncioTestCase):
    async def test_aad_scope_override_async(self):
        os.environ["AZURE_COSMOS_AAD_SCOPE_OVERRIDE"] = "https://cosmos.azure.com/.default"

        host1 = "https://b9a82029-f577-44d8-99ec-939209dd0c1c.zb9.msit-sql.cosmos.fabric.microsoft.com:443/"
        host2 = "https://aadaudiencetest.documents.azure.com:443/"
        database_id = "TestMsFabricDb"
        container_id = "TestMsFabricContainerNew"
        partition_key_value = "partition1"

        credential = DefaultAzureCredential()
        scope = "https://cosmos.azure.com/.default"

        # Log and compare tokens
        token1 = await log_token(credential, scope, "token1")
        token2 = await log_token(credential, scope, "token2")
        token3 = await log_token(credential, scope, "token3")

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
