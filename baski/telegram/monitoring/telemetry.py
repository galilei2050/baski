from aiogram import types
from baski.monitoring import Telemetry


__all__ = ['MessageTelemetry']


class MessageTelemetry(Telemetry):

    def add_message(self, event_type, message: types.Message, user: types.User):
        user_id = user.id
        timestamp = message.date
        self.add(user_id, event_type, self.message_payload(message, user), timestamp=timestamp)

    def message_payload(self, message: types.Message, user: types.User):
        data = {
            "content_type": str(message.content_type),
            "username": user.username,
            "language_code": user.language_code,
            "is_premium": user.is_premium,
            "user_id": user.id,

        }
        if message.chat:
            data = data | {
                "chat_type": message.chat.type
            }

        if message.text:
            data = data | {
                "text_letters_cnt": len(message.text),
                "text_words_cnt": message.text.count(" ") + 1,
            }
        return data
