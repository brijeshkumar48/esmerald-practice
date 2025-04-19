import os
import pytest
import httpx

UPLOAD_DIR = "uploads/files"


class TestUploadFile:
    """
    Test uploading a file to the running API.
    """

    def teardown_method(self):
        uploaded_path = os.path.join(UPLOAD_DIR, "sample.txt")
        if os.path.exists(uploaded_path):
            os.remove(uploaded_path)

    @pytest.mark.asyncio
    async def test_file_upload(self, upload_url, test_file_path):
        async with httpx.AsyncClient() as client:
            with open(test_file_path, "rb") as f:
                response = await client.post(
                    upload_url,
                    files={"path": ("sample.txt", f, "text/plain")},
                    data={"type": "text"}
                )

        assert response.status_code == 201
        res_json = response.json()

        assert res_json["message"] == "Ok"
        assert "relative_path" in res_json["data"]

        uploaded_path = os.path.join(UPLOAD_DIR, "sample.txt")
        assert os.path.exists(uploaded_path)


class TestDeleteFile:
    @pytest.mark.asyncio
    async def test_file_delete(self, upload_url, test_file_path):
        pass
