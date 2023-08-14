from aiogram import types
from baski.monitoring import Telemetry


__all__ = ['MessageTelemetry']


class MessageTelemetry(Telemetry):

    def add_message(self, event_type, message: types.Message):
        user_id = message.from_user.id
        timestamp = message.date
        self.add(user_id, event_type, self.message_payload(message), timestamp=timestamp)

    def message_payload(self, message: types.Message):
        data = {
            "content_type": str(message.content_type),
        }
        if message.chat:
            data = data | {
                "chat_type": message.chat.type
            }

        if message.text:
            data = data | {
                "text_length": len(message.text),
                "text_words": message.text.count(" "),
            }

        if message.from_user:
            user = message.from_user
            data = data | {
                "username": user.username,
                "lauguage_code": user.language_code,
                "is_premium": user.is_premium,
            }
        return data
