import abc
import httpx
import logging
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)

class LLMAdapter(abc.ABC):
    @abc.abstractmethod
    async def chat_completion(
        self, 
        messages: List[Dict[str, str]], 
        api_key: str, 
        model: str, 
        base_url: Optional[str] = None
    ) -> Optional[str]:
        pass

    @abc.abstractmethod
    async def list_models(self, api_key: str, base_url: Optional[str] = None) -> List[str]:
        pass

class GroqAdapter(LLMAdapter):
    async def chat_completion(self, messages, api_key, model, base_url=None):
        url = base_url or "https://api.groq.com/openai/v1/chat/completions"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {"model": model, "messages": messages, "temperature": 0.1}
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]


    async def list_models(self, api_key, base_url=None):
        url = base_url or "https://api.groq.com/openai/v1/models"
        headers = {"Authorization": f"Bearer {api_key}"}
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            return [m["id"] for m in data.get("data", [])]

class OpenAIAdapter(LLMAdapter):
    async def chat_completion(self, messages, api_key, model, base_url=None):
        url = base_url or "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {"model": model, "messages": messages, "temperature": 0.1}
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]

    async def list_models(self, api_key, base_url=None):
        url = base_url or "https://api.openai.com/v1/models"
        headers = {"Authorization": f"Bearer {api_key}"}
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            # Filter for text/chat models for OpenAI
            return [m["id"] for m in data.get("data", []) if "gpt" in m["id"] or "o1" in m["id"]]

class GeminiAdapter(LLMAdapter):
    async def chat_completion(self, messages, api_key, model, base_url=None):
        # Gemini OpenAI compatibility endpoint
        url = base_url or f"https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {"model": model, "messages": messages, "temperature": 0.1}
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]

    async def list_models(self, api_key, base_url=None):
        # Gemini OpenAI compatibility list models
        url = base_url or "https://generativelanguage.googleapis.com/v1beta/openai/v1/models"
        headers = {"Authorization": f"Bearer {api_key}"}
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
                return [m["id"] for m in data.get("data", [])]
            except Exception:
                # Fallback to a hardcoded stable list if API fails
                return ['gemini-2.0-flash-exp', 'gemini-1.5-flash', 'gemini-1.5-pro']

class DeepSeekAdapter(LLMAdapter):
    async def chat_completion(self, messages, api_key, model, base_url=None):
        url = base_url or "https://api.deepseek.com/chat/completions"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {"model": model, "messages": messages, "temperature": 0.1}
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]

    async def list_models(self, api_key, base_url=None):
        url = base_url or "https://api.deepseek.com/models"
        headers = {"Authorization": f"Bearer {api_key}"}
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            return [m["id"] for m in data.get("data", [])]

class AnthropicAdapter(LLMAdapter):
    async def chat_completion(self, messages, api_key, model, base_url=None):
        url = base_url or "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json"
        }
        # Convert OpenAI-style messages to Anthropic style
        system_msg = next((m["content"] for m in messages if m["role"] == "system"), None)
        user_msgs = [m for m in messages if m["role"] != "system"]
        
        payload = {
            "model": model,
            "messages": user_msgs,
            "max_tokens": 1024,
            "temperature": 0.1
        }
        if system_msg:
            payload["system"] = system_msg

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            return data["content"][0]["text"]

    async def list_models(self, api_key, base_url=None):
        # Anthropic doesn't have a public discovery API for all models yet.
        # They require hardcoding or checking their documentation.
        return ['claude-3-5-sonnet-latest', 'claude-3-5-haiku-latest', 'claude-3-opus-latest']

class CustomAdapter(LLMAdapter):
    async def chat_completion(self, messages, api_key, model, base_url=None):
        if not base_url:
            raise ValueError("Base URL is required for Custom adapter.")
        # Custom provider usually follows OpenAI standard
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {"model": model, "messages": messages, "temperature": 0.1}
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(base_url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]

    async def list_models(self, api_key, base_url=None):
        if not base_url: return []
        # Try OpenAI standard models endpoint
        url = base_url.replace("/chat/completions", "/models")
        headers = {"Authorization": f"Bearer {api_key}"}
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    return [m["id"] for m in data.get("data", [])]
        except Exception:
            pass
        return []

def get_adapter(provider: str) -> LLMAdapter:
    provider = provider.lower()
    if provider == "groq": return GroqAdapter()
    if provider == "openai": return OpenAIAdapter()
    if provider == "gemini": return GeminiAdapter()
    if provider == "deepseek": return DeepSeekAdapter()
    if provider == "anthropic": return AnthropicAdapter()
    if provider == "custom": return CustomAdapter()
    raise ValueError(f"Provider {provider} not supported.")
