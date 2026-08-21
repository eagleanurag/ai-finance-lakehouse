from __future__ import annotations

import time
from typing import Any

import requests

from config import settings

RETRYABLE_STATUS = {401, 402, 403, 408, 409, 425, 429, 500, 502, 503, 504}


PROVIDERS: dict[str, dict[str, str]] = {
    "groq": {
        "label": "Groq (GPT-OSS 120B)",
        "base_url": "https://api.groq.com/openai/v1/chat/completions",
        "env_var": "GROQ_API_KEY",
        "model_setting": "GROQ_MODEL",
    },
    "gemini": {
        "label": "Google Gemini Flash",
        "base_url": "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
        "env_var": "GEMINI_API_KEY",
        "model_setting": "GEMINI_MODEL",
    },
    "mistral": {
        "label": "Mistral Small",
        "base_url": "https://api.mistral.ai/v1/chat/completions",
        "env_var": "MISTRAL_API_KEY",
        "model_setting": "MISTRAL_MODEL",
    },
    "openrouter": {
        "label": "OpenRouter (Free models)",
        "base_url": "https://openrouter.ai/api/v1/chat/completions",
        "env_var": "OPENROUTER_API_KEY",
        "model_setting": "OPENROUTER_MODEL",
    },
}


def _provider(key: str) -> tuple[str, str]:
    spec = PROVIDERS[key]
    api_key = _get_secret(spec["env_var"])
    model = _get_secret(spec["model_setting"])
    return api_key, model


def _get_secret(name: str) -> str:
    value = getattr(settings, name, "")
    if not value:
        try:
            import streamlit as st

            value = st.secrets.get(name, "")
        except Exception:
            pass
    return str(value) if value else ""


def available_providers() -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for key in settings.PROVIDER_ORDER:
        if key not in PROVIDERS:
            continue
        api_key, model = _provider(key)
        if api_key:
            out.append(
                {
                    "key": key,
                    "label": f"{PROVIDERS[key]['label']} ({model})",
                    "model": model,
                }
            )
    return out


def chat(
    messages: list[dict[str, str]],
    provider_key: str = "auto",
    temperature: float = 0.1,
    max_tokens: int = 1500,
    timeout: int = 90,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    chain = (
        [provider_key]
        if provider_key != "auto"
        else [p["key"] for p in available_providers()]
    )
    if not chain:
        return {
            "content": None,
            "error": (
                "No LLM providers configured. Add at least one free API key "
                "(GROQ_API_KEY / GEMINI_API_KEY / MISTRAL_API_KEY / GITHUB_MODELS_TOKEN) "
                "to .env or Streamlit secrets."
            ),
            "attempts": attempts,
        }

    last_error: str | None = None
    for key in chain:
        api_key, model = _provider(key)
        url = PROVIDERS[key]["base_url"]
        started = time.time()
        try:
            resp = requests.post(
                url,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": messages,
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                },
                timeout=timeout,
            )
            latency_ms = int((time.time() - started) * 1000)
            if resp.status_code in RETRYABLE_STATUS:
                detail = _extract_error(resp)
                attempts.append(
                    {"provider": key, "model": model, "status": resp.status_code, "detail": detail}
                )
                last_error = f"{key}: HTTP {resp.status_code} - {detail}"
                continue
            resp.raise_for_status()
            payload = resp.json()
            content = payload["choices"][0]["message"]["content"]
            attempts.append({"provider": key, "model": model, "status": 200, "latency_ms": latency_ms})
            return {
                "content": content,
                "provider": key,
                "provider_label": PROVIDERS[key]["label"],
                "model": model,
                "latency_ms": latency_ms,
                "attempts": attempts,
            }
        except requests.RequestException as exc:
            attempts.append({"provider": key, "model": model, "status": "network_error", "detail": str(exc)})
            last_error = f"{key}: {exc}"
            continue

    return {"content": None, "error": f"All providers exhausted. Last error -> {last_error}", "attempts": attempts}


def _extract_error(resp: requests.Response) -> str:
    try:
        body = resp.json()
        err = body.get("error", {})
        if isinstance(err, dict):
            return err.get("message", resp.text[:200])
        return str(err)[:200]
    except Exception:
        return resp.text[:200]


def simple_completion(prompt: str, system: str, provider_key: str = "auto") -> dict[str, Any]:
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": prompt},
    ]
    return chat(messages, provider_key=provider_key)

