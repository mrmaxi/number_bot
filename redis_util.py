import json
from redis import StrictRedis
from collections import defaultdict
from typing import Optional, Union, Iterable, List
import logging

logger = logging.getLogger(__name__)


class ValueNotExists(object):
    pass


value_not_exists = ValueNotExists()


def prepare_value_for_json(value: any) -> Optional[Union[int, float, bool, str]]:
    """
    convert to str datatypes not suitable for json serialization
    """

    if value is None:
        return value
    elif isinstance(value, (int, float, bool, str)):
        return value
    else:
        return str(value)


def prepare_obj_for_json(obj: any) -> Optional[Union[int, float, bool, str, dict, list]]:
    """
    convert values of complex datatypes (dicts, lists) not suitable for json serialization to str
    """

    if isinstance(obj, dict):
        return {prepare_value_for_json(key): prepare_obj_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [prepare_obj_for_json(value) for value in obj]
    else:
        return prepare_value_for_json(obj)


def redis_from_url_or_object(redis_url: Union[str, 'StrictRedis']) -> StrictRedis:
    """
    return redis object if url passed
    """

    if isinstance(redis_url, StrictRedis):
        return redis_url
    elif isinstance(redis_url, str):
        return StrictRedis.from_url(redis_url, decode_responses=True)
    else:
        assert False, f'redis_url must be Redis object or url, not {type(redis_url)}'


class RedisDict(dict):
    """
        Dictionary, that store in Redis as one solid json by his own redis key (key_id)
        for save dict in redis it's need to call :meth:`telegram.ext.redis_util.RedisDict.flush`
        read object from redis on initialization :meth:`telegram.ext.redis_util.RedisDict.__init__`
    """

    def __init__(self, redis_url: Union[str, 'StrictRedis'], key_id: str, seq: Optional[Iterable] = None, **kwargs):
        self._redis = redis_from_url_or_object(redis_url)
        self.key_id = key_id
        args = [] if seq is None else [seq]
        super().__init__(*args, **kwargs)
        if not self:
            self.read()

    def read(self):
        self.clear()
        self.update(json.loads(self._redis.get(self.key_id) or '{}'))

    def flush(self):
        obj = prepare_obj_for_json(self)
        self._redis.set(self.key_id, json.dumps(obj))


class BaseRedisStore(defaultdict):
    """
        Dictionary that store values in Redis, every by his own key as key_id:key
        It's using 'lazy read' from Redis, so read key value only when key is requested
        All keys convert to str
    """

    def key2id(self, key: any) -> str:
        return f"{self.key_id}:{key}"

    def id2key(self, key_id: str) -> str:
        return key_id[len(self.key_id) + 1:]

    @staticmethod
    def serialize(key: any, value: any) -> str:
        return json.dumps(value)

    @staticmethod
    def deserialize(key: any, serialized_value: str) -> any:
        return json.loads(serialized_value)

    def __read_from_redis__(self, key: any) -> any:
        serialized_value = self._redis.get(self.key2id(key))
        if serialized_value is None:
            return value_not_exists
        value = self.deserialize(key, serialized_value)
        logger.debug(f'read {key} from redis = {value}')
        return value

    def __save_to_redis__(self, key: any, value: any) -> any:
        serialized_value = self.serialize(key, value)
        self._redis.set(self.key2id(key), serialized_value)
        return value

    def __remove_from_redis__(self, key: any) -> None:
        self._redis.delete(self.key2id(key))

    def __exists_in_redis__(self, key: any) -> bool:
        logger.debug(f'check {key} in redis')
        return self._redis.exists(self.key2id(key))

    def __read_keys_from_redis__(self) -> List[any]:
        return [self.id2key(key_id) for key_id in self._redis.keys(f'{self.key_id}:*')]

    def __read_throw_redis__(self, key: any) -> any:
        key = str(key)
        value = self.__read_from_redis__(key)
        if value is not value_not_exists:
            super().__setitem__(key, value)
        return value

    def __save_throw_redis__(self, key: any, value: any) -> any:
        key = str(key)
        value = self.__save_to_redis__(key, value)
        super().__setitem__(key, value)
        return value

    def flush(self) -> None:
        pass

    def free(self, key: any) -> None:
        super().__delitem__(key)

    def __init__(self, redis_url: Union[str, 'StrictRedis'], key_id: str, default_factory=None, lazy_read=True, seq=None):
        self.key_id = key_id
        self._redis = redis_from_url_or_object(redis_url)

        args = []
        if seq is not None:
            args = [seq]
        elif not lazy_read:
            seq = [(key, self.__read_from_redis__(key)) for key in self.__read_keys_from_redis__()]
            args = [seq]
        super().__init__(default_factory, *args)

    def get(self, key: any, default: Optional[any] = None):
        key = str(key)
        if key in self:
            return self[key]

        value = self.__read_throw_redis__(key)
        if value is not value_not_exists:
            return value

        return default

    def setdefault(self, key: any, default: Optional[any] = None):
        key = str(key)
        if key in self:
            return self[key]

        value = self.__read_throw_redis__(key)
        if value is not value_not_exists:
            return value

        return self.__save_throw_redis__(key, default)

    def __missing__(self, key: any) -> any:
        key = str(key)

        value = self.__read_throw_redis__(key)
        if value is not value_not_exists:
            return value

        return self.__save_throw_redis__(key, self.default_factory())

    def __setitem__(self, key: any, value: any) -> None:
        self.__save_throw_redis__(key, value)

    def __delitem__(self, key: any) -> None:
        self.__remove_from_redis__(key)
        super().__delitem__(key)

    def __iter__(self):
        return iter(self.keys() | self.__read_keys_from_redis__())

    def __copy__(self):
        return self.__class__(self._redis, self.key_id, default_factory=self.default_factory, seq=self.items())


class RedisDictStore(BaseRedisStore):
    """
        Dictionary that store many dicts, every by his own key in Redis
        Every dict is RedisDict - dict that store as solid json
        It's using 'lazy read' from BaseRedisStore
    """

    def __init__(self, redis_url: Union[str, 'StrictRedis'], key_id: str, default_factory=lambda: dict(), lazy_read=True, seq=None):
        super().__init__(redis_url, key_id, default_factory=default_factory, lazy_read=lazy_read, seq=seq)

    def __read_from_redis__(self, key: any) -> any:
        if self.__exists_in_redis__(key):
            return RedisDict(self._redis, self.key2id(key))
        else:
            return value_not_exists

    def __save_to_redis__(self, key: any, value: dict) -> RedisDict:
        if not isinstance(value, RedisDict):
            assert isinstance(value, dict), f'item value of RedisDictStore must be a dict, not {type(value)}'
            value = RedisDict(self._redis, self.key2id(key), value.items())
        value.flush()
        return value

    def flush(self) -> None:
        for value in self.values():
            value.flush()


class RedisSimpleStore(BaseRedisStore):
    """
        Dictionary that store many values in Redis
        Every value is stored as json by his own key in Redis
        Don't use 'lazy read' by default, immediate read all keys from Redis on initialization
        keys converting to json, so it suitable for use tuple as keys,
        tuple encoding to list when storing and decoding to tuple when reading
    """

    def key2id(self, key: any) -> str:
        return f"{self.key_id}:{json.dumps(key)}"

    def id2key(self, key_id: str) -> any:
        key = json.loads(key_id[len(self.key_id) + 1:])
        if isinstance(key, list):
            key = tuple(key)
        return key

    def __init__(self, redis_url: Union[str, 'StrictRedis'], key_id: str, default_factory=lambda: 0, lazy_read=True, seq=None):
        super().__init__(redis_url, key_id, default_factory=default_factory, lazy_read=lazy_read, seq=seq)
