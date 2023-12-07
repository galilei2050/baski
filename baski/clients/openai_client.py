import logging
import typing
import aiohttp
import openai
import asyncio
import io
import tiktoken
import json
from copy import deepcopy

from openai.openai_object import OpenAIObject
from baski import monitoring, pattern


__all__ = ["OpenAiClient"]

OPENAI_INPUT_TEXT = "openai_in_text"
OPENAI_OUTPUT_TEXT = "openai_out_text"


class OpenAiClient(object):
    _retry_exceptions = (
        openai.error.APIError,
        openai.error.Timeout,
        openai.error.APIConnectionError,
        openai.error.ServiceUnavailableError,
        json.decoder.JSONDecodeError,
        aiohttp.ClientError,
        asyncio.exceptions.TimeoutError
    )

    def __init__(
            self, api_key, system_prompt, user_prompts=None, default_cgi=None, chunk_length=128,
            telemetry: monitoring.Telemetry=None
    ):
        openai.api_key = api_key
        self.system_prompt = system_prompt
        self.user_prompts = user_prompts or {}
        self.default_cgi = default_cgi or _CGI
        self.chunk_length = chunk_length
        self.token_encoder = tiktoken.get_encoding("cl100k_base")
        self.telemetry = telemetry

    async def transcribe(self, user_id, audio: io.FileIO) -> typing.AnyStr:
        result = await pattern.retry(
            openai.Audio.atranscribe,
            exceptions=self._retry_exceptions,
            service_name="Open AI",
            model="whisper-1", file=audio
        )
        text = result['text']
        self._log_response(user_id, text, "transcribe", "whisper-1")
        return text

    def from_prompt(self, user_id, prompt, history=None, prepend=False, streaming=True, **params):
        if streaming:
            return self.from_prompt_streaming(user_id, prompt, history, prepend, **params)
        else:
            return self.from_prompt_gather(user_id, prompt, history, prepend, **params)

    async def from_prompt_gather(self, user_id, prompt, history=None, prepend=False, **params):
        result = None
        async for chunk in self.from_prompt_streaming(user_id, prompt, history, prepend, **params):
            result = chunk
        return result

    def from_prompt_streaming(self, user_id, prompt, history=None, prepend=False, **params):
        history = [_check_message(msg) for msg in history or []]
        prompt_text, prompt_cfg, request_id = self._get_prompt_text_cfg(prompt, **params)

        message = from_user(prompt_text)
        return self._create_message(
            user_id=user_id,
            history=[message] + history if prepend else history + [message],
            request_id=request_id,
            **prompt_cfg
        )

    def _get_prompt_text_cfg(self, prompt, **params):
        prompt_cfg, request_id = {}, "custom"
        if prompt in self.user_prompts:
            request_id = prompt
            prompt_cfg = deepcopy(self.user_prompts[prompt])
            prompt_text = prompt_cfg.pop('prompt')
            if params:
                prompt_text = prompt_text.format(**params)
        else:
            prompt_text = prompt
        return prompt_text, prompt_cfg, request_id

    async def _create_message(self, user_id, history, request_id, **params):
        assert isinstance(history, list)
        messages = [_from_system(self.system_prompt)] + history
        this_cgi = self.default_cgi.copy() | params
        self._log_request(user_id, messages, request_id, this_cgi.get('model', 'undefined'))
        for i in range(1, 50):
            try:
                this_cgi = self.default_cgi.copy() | params
                response: OpenAIObject = await openai.ChatCompletion.acreate(
                    messages=messages,
                    user=str(user_id),
                    stream=True,
                    **this_cgi
                )
                chunks = []
                yielded_text = ""
                async for chunk in response:
                    if chunk['choices'][0]['finish_reason'] == "stop":
                        break
                    delta = chunk['choices'][0]['delta']
                    logging.debug(f"new chunk: {delta.get('content', '')}")
                    content = delta.get('content', '')
                    chunks.append(content)
                    size_so_far = sum(len(c) for c in chunks)
                    if '\n' in content or size_so_far - len(yielded_text) > self.chunk_length:
                        yielded_text = ''.join(chunks)
                        yield yielded_text
                final_text = ''.join(chunks)
                if final_text != yielded_text:
                    yield final_text
                self._log_response(user_id, final_text, request_id, this_cgi.get('model', 'undefined'))
                return
            except openai.error.InvalidRequestError as e:
                raise
            except (openai.error.APIError,
                    openai.error.Timeout,
                    openai.error.APIConnectionError,
                    openai.error.ServiceUnavailableError,
                    json.decoder.JSONDecodeError,
                    aiohttp.ClientError,
                    asyncio.exceptions.TimeoutError) as e:
                logging.warning(f"{i} Get exception from OpenAI: {e}")
                try:
                    await asyncio.sleep(i)
                except asyncio.exceptions.CancelledError:
                    logging.warning("OpenAI request is cancelled")
                    return
        raise RuntimeError("OpenAI is not available")

    def _log_request(self, user_id, messages, request_id, model):
        if not self.telemetry:
            return
        self.telemetry.add(
            user_id=user_id,
            event_type=OPENAI_INPUT_TEXT,
            payload={
                "request_id": request_id,
                "tokens": sum([len(self.token_encoder.encode(msg['content'])) for msg in messages]),
                "model": model
            }
        )

    def _log_response(self, user_id, text, request_id, model):
        if not self.telemetry:
            return
        self.telemetry.add(
            user_id=user_id,
            event_type=OPENAI_OUTPUT_TEXT,
            payload={
                "request_id": request_id,
                "tokens": len(self.token_encoder.encode(text)),
                "model": model
            }
        )


def _check_message(msg):
    assert isinstance(msg, dict), f"Message must be dict, got {type(msg)}"
    assert set(msg.keys()) == {"role", "content"}, f"Message must have keys 'role' and 'content', got {msg.keys()}"
    assert msg['role'] in {"user", "assistant"}, f"Message role must be 'user', 'assistant', got {msg['role']}"
    return msg


def _from_system(msg):
    return {"role": "system", "content": msg.strip()}


def from_user(msg):
    return {"role": "user", "content": msg}


def from_ai(msg):
    return {"role": "assistant", "content": msg}


_CGI = {
    "model": "gpt-3.5-turbo",
    "n": 1,
    "temperature": 1.0,
    "timeout": 20 * 60,
    "request_timeout": 5 * 60,
}
