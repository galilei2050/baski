"""Storage backends for Telegram users and FSM state."""

from .firebase import FirebaseStorage as FirebaseStorage
from .users import TelegramUser as TelegramUser
from .users import UsersStorage as UsersStorage
