import logging
import typing
import aiohttp
import openai
import asyncio
import io

from copy import deepcopy

from openai.openai_object import OpenAIObject


__all__ = ["OpenAiClient"]


class OpenAiClient(object):

    def __init__(self, api_key, system_prompt, user_prompts=None, default_cgi=None, chunk_length=128):
        openai.api_key = api_key
        self.system_prompt = system_prompt
        self.user_prompts = user_prompts or {}
        self.default_cgi = default_cgi or _CGI
        self.chunk_length = chunk_length

    async def transcribe(self, audio: io.FileIO) -> typing.AnyStr:
        result = await openai.Audio.atranscribe("whisper-1", file=audio)
        return result['text']

    def from_prompt(self, user_id, prompt, history=None, prepend=False, **params):
        history = [_check_message(msg) for msg in history or []]
        prompt_cfg, request_id = {}, "custom"
        if prompt in self.user_prompts:
            request_id=prompt
            prompt_cfg = deepcopy(self.user_prompts[prompt])
            prompt_text = prompt_cfg.pop('prompt')
            if params:
                prompt_text = prompt_text.format(**params)
        else:
            prompt_text = prompt

        message = from_user(prompt_text)
        return self._create_message(
            user_id=user_id,
            history=[message] + history if prepend else history + [message],
            request_id=request_id,
            **prompt_cfg
        )

    async def _create_message(self, user_id, history, request_id, **params):
        assert isinstance(history, list)
        messages = [_from_system(self.system_prompt)] + history
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
                return
            except openai.error.InvalidRequestError as e:
                raise RuntimeError(f"Invalid request \"{request_id}\" for user {user_id}: {e}")
            except (openai.error.APIError,
                    openai.error.RateLimitError,
                    openai.error.Timeout,
                    openai.error.ServiceUnavailableError,
                    aiohttp.ClientError,
                    asyncio.exceptions.TimeoutError) as e:
                logging.warning(f"{i} Get exception from OpenAI: {e}")
                await asyncio.sleep(i)
        raise RuntimeError("OpenAI is not available")


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
    "model": "gpt-3.5-turbo-0613",
    "n": 1,
    "temperature": 1.0,
    "timeout": 10 * 60,
    "request_timeout": 1 * 60,
}
