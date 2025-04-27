import pytest
from unittest.mock import patch
from app.llm_service import call_llm
from unittest.mock import patch, AsyncMock
from main import app
from esmerald.testclient import EsmeraldTestClient


class TestMathOperations:
    """
    Test basic math operations.
    """

    def teardown_method(self):
        """
        Teardown after each test method.
        (No cleanup needed for calculation, but showing the structure.)
        """
        pass

    @pytest.mark.asyncio
    async def test_generate_response(monkeypatch):
        fake_llm_response = "This is a fake LLM response."

        mock_llm = AsyncMock(return_value=fake_llm_response)

        with patch("app.views.call_llm", mock_llm):
            client = EsmeraldTestClient(app)
            response = client.post(
                "/api/llm/generate",
                json={
                    "system_prompt": "You are a helpful AI.",
                    "user_message": "Hello, how are you?",
                },
                headers={"Content-Type": "application/json"},
            )

        assert response.status_code == 201
        assert response.json()["data"]["response"] == fake_llm_response

    @pytest.mark.asyncio
    async def test_addition(self):
        """
        Test asynchronous addition logic.
        """
        result = await self.async_add(2, 3)
        assert result == 5

    @pytest.mark.asyncio
    async def test_multiplication(self):
        """
        Test asynchronous multiplication logic.
        """
        result = await self.async_multiply(4, 5)
        assert result == 20

    async def async_add(self, a, b):
        return a + b

    async def async_multiply(self, a, b):
        return a * b
