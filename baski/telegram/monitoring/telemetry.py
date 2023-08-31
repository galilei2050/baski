from aiogram import types
from baski.monitoring import Telemetry


__all__ = ['MessageTelemetry']


class MessageTelemetry(Telemetry):

    def add_message(self, event_type, message: types.Message, user: types.User, **payload):
        user_id = user.id
        timestamp = message.date
        self.add(user_id, event_type, self.message_payload(message, user) | payload, timestamp=timestamp)

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
        if message.video:
            data = data | {
                "video_duration": message.video.duration,
                "video_file_size": message.video.file_size,
                "video_width": message.video.width,
                "video_height": message.video.height
            }
        if message.voice:
            data = data | {
                "voice_duration": message.voice.duration,
                "voice_file_size": message.voice.file_size
            }
        if message.audio:
            data = data | {
                "audio_duration": message.audio.duration,
                "audio_file_size": message.audio.file_size
            }
        if message.photo:
            data = data | {
                "photo_width": message.photo[0].width,
                "photo_height": message.photo[0].height,
                "photo_file_size": message.photo[0].file_size,
                "photo_cnt": len(message.photo)
            }

        return data
