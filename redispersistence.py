from typing import DefaultDict, Dict, Any, Tuple, Optional, Union

from telegram.ext.basepersistence import BasePersistence
from redis_util import RedisDictStore, RedisSimpleStore, RedisDict, redis_from_url_or_object, StrictRedis
from telegram.utils.types import ConversationDict

import logging

logger = logging.getLogger(__name__)


class RedisPersistence(BasePersistence):
    """Using Redis for making your bot persistent.

        Note:
            This class store data in Redis in json, so it's necessary to use compatible with
            JSON-serialization types of data.

        Warning:
            :class:`RedisPersistence` will try to replace :class:`telegram.Bot` instances by
            :attr:`REPLACED_BOT` and insert the bot set with
            :meth:`telegram.ext.BasePersistence.set_bot` upon loading of the data. This is to ensure
            that changes to the bot apply to the saved objects, too. If you change the bots token, this
            may lead to e.g. ``Chat not found`` errors. For the limitations on replacing bots see
            :meth:`telegram.ext.BasePersistence.replace_bot` and
            :meth:`telegram.ext.BasePersistence.insert_bot`.

        Attributes:
            store_user_data (:obj:`bool`): Whether user_data should be saved by this
                persistence class.
            store_chat_data (:obj:`bool`): Whether chat_data should be saved by this
                persistence class.
            store_bot_data (:obj:`bool`): Whether bot_data should be saved by this
                persistence class.

        Args:
            redis_url (:obj:`str` | :obj:`Redis`): Redis object or url (for example redis://127.0.0.1) of Redis server
            bot_id (:obj:`str`, optional): global prefix for key names when it's stored into Redis
            store_user_data (:obj:`bool`, optional): Whether user_data should be saved by this
                persistence class. Default is :obj:`True`.
            store_chat_data (:obj:`bool`, optional): Whether user_data should be saved by this
                persistence class. Default is :obj:`True`.
            store_bot_data (:obj:`bool`, optional): Whether bot_data should be saved by this
                persistence class. Default is :obj:`True` .
        """

    def __init__(self,
                 redis_url: Union[str, 'StrictRedis'],
                 bot_id: Optional[str] = None,
                 store_user_data: bool = True,
                 store_chat_data: bool = True,
                 store_bot_data: bool = True):
        super().__init__(store_user_data=store_user_data,
                         store_chat_data=store_chat_data,
                         store_bot_data=store_bot_data)
        self.id_prefix = f'bot_{bot_id}:' if bot_id else ''
        self._redis = redis_from_url_or_object(redis_url)
        self._bot_data = RedisDict(self._redis, f'{self.id_prefix}bot_data')
        self._user_data = RedisDictStore(self._redis, f'{self.id_prefix}user_data')
        self._chat_data = RedisDictStore(self._redis, f'{self.id_prefix}chat_data')

        self._conversations = dict()

    @property
    def user_data(self) -> Optional[DefaultDict[int, Dict]]:
        """:obj:`dict`: The user_data as a dict."""
        return self._user_data

    @property
    def chat_data(self) -> Optional[DefaultDict[int, Dict]]:
        """:obj:`dict`: The chat_data as a dict."""
        return self._chat_data

    @property
    def bot_data(self) -> Optional[Dict]:
        """:obj:`dict`: The bot_data as a dict."""
        return self._bot_data

    @property
    def conversations(self) -> Optional[Dict[str, Dict[Tuple, Any]]]:
        """:obj:`dict`: The conversations as a dict."""
        return self._conversations

    def get_user_data(self) -> DefaultDict[int, Dict[Any, Any]]:
        return self.user_data

    def get_chat_data(self) -> DefaultDict[int, Dict[Any, Any]]:
        return self.chat_data

    def get_bot_data(self) -> Dict[Any, Any]:
        return self.bot_data

    def get_conversations(self, name: str) -> ConversationDict:
        conversation = self.conversations.get(name, None)
        if conversation is None:
            conversation = RedisSimpleStore(redis_url=self._redis, key_id=f'{self.id_prefix}conversations:{name}')
            self.conversations[name] = conversation

        return conversation

    def update_conversation(self,
                            name: str, key: Tuple[int, ...],
                            new_state: Optional[object]) -> None:
        """Will update the conversations for the given handler.

            Args:
                name (:obj:`str`): The handler's name.
                key (:obj:`tuple`): The key the state is changed for.
                new_state (:obj:`tuple` | :obj:`any`): The new state for the given key.
            """
        conversation = self.get_conversations(name)
        conversation[key] = new_state

    def update_user_data(self, user_id: int, data: Dict) -> None:
        """Will update the user_data (if changed).

            Args:
                user_id (:obj:`int`): The user the data might have been changed for.
                data (:obj:`dict`): The :attr:`telegram.ext.dispatcher.user_data` [user_id].
            """
        if isinstance(data, RedisDict):
            data.flush()
        else:
            self.user_data[user_id] = data

    def update_chat_data(self, chat_id: int, data: Dict) -> None:
        """Will update the chat_data (if changed).

            Args:
                chat_id (:obj:`int`): The chat the data might have been changed for.
                data (:obj:`dict`): The :attr:`telegram.ext.dispatcher.chat_data` [chat_id].
            """
        if isinstance(data, RedisDict):
            data.flush()
        else:
            self.chat_data[chat_id] = data

    def update_bot_data(self, data: Dict) -> None:
        """Will update the bot_data (if changed).

            Args:
                data (:obj:`dict`): The :attr:`telegram.ext.dispatcher.bot_data`.
            """
        if isinstance(data, RedisDict):
            data.flush()
        else:
            self._bot_data = RedisDict(self._redis, f'{self.id_prefix}bot_data', data.items())

    def flush(self) -> None:
        """Will be called by :class:`telegram.ext.Updater` upon receiving a stop signal. Gives the
            persistence a chance to finish up saving or close a database connection gracefully. If this
            is not of any importance just pass will be sufficient.
            """

        if self.store_user_data:
            self.user_data.flush()

        if self.store_chat_data:
            self.chat_data.flush()

        if self.store_bot_data:
            self.bot_data.flush()

        for conversation in self.conversations.values():
            conversation.flush()
