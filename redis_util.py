import json
from redis import StrictRedis
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


def prepare_value_for_json(value):
    if value is None:
        return value
    elif isinstance(value, (int, float, bool, str)):
        return value
    else:
        return str(value)


def prepare_obj_for_json(obj):
    if isinstance(obj, dict):
        return {prepare_value_for_json(key): prepare_obj_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [prepare_obj_for_json(value) for value in obj]
    else:
        return prepare_value_for_json(obj)


class RedisDict(dict):
    """
        Dictionary, that store in Redis as one solid json by his own key
        for save dict in redis it's need to call .save() method
        when __init__ called read object from redis occurred by default
            Redis object will be overridden if 'source' argument passed
    """

    _redis = None
    id = None

    def __init__(self, redis, id, seq=None, **kwargs):
        self._redis = redis
        self.id = id
        args = [] if seq is None else [seq]
        super().__init__(*args, **kwargs)
        if not self:
            self.read()

    def read(self):
        self.clear()
        self.update(json.loads(self._redis.get(self.id) or '{}'))

    def flush(self):
        obj = prepare_obj_for_json(self)
        self._redis.set(self.id, json.dumps(obj))


class BaseRedisStore(defaultdict):
    """
        Dictionary that store several values of same type, every by his own key in Redis
        Every value is the same type, for example RedisDict that store as solid json
        It's used 'lazy read' from Redis, only when key is requested
        All keys convert to str

        it's need to override __read_from_redis__, __save_to_redis__
    """

    _redis = None
    id = None

    def key2id(self, key):
        return f"{self.id}:{key}"

    def id2key(self, id):
        return id[len(self.id) + 1:]

    def default_factory_from_key(self, key):
        return None

    @staticmethod
    def serialize(key, value) -> str:
        return json.dumps(value)

    @staticmethod
    def deserialize(key, serialized_value: str):
        return json.loads(serialized_value)

    def __read_from_redis__(self, key):
        serialized_value = self._redis.get(self.key2id(key))
        value = self.deserialize(key, serialized_value)
        return value

    def __save_to_redis__(self, key, value):
        serialized_value = self.serialize(key, value)
        self._redis.set(self.key2id(key), serialized_value)
        return value

    def __remove_from_redis__(self, key):
        self._redis.delete(self.key2id(key))

    def __exists_in_redis__(self, key):
        return self._redis.exists(self.key2id(key))

    def __read_keys_from_redis__(self):
        return [self.id2key(id) for id in self._redis.keys(f'{self.id}:*')]

    def flush(self):
        pass

    def __init__(self, redis_url, id, default_factory=None, lazy_read=True):
        self.id = id
        self._redis = StrictRedis.from_url(redis_url, decode_responses=True)
        args = []
        if not lazy_read:
            args = [(key, self.__read_from_redis__(key)) for key in self.__read_keys_from_redis__()]
        super().__init__(default_factory, *args)

    def __missing__(self, key):
        key = str(key)
        logger.debug(f'check {key} in redis')
        if self.__exists_in_redis__(key):
            value = self.__read_from_redis__(key)
            logger.debug(f'read {key} from redis = {value}')
        else:
            value = self.default_factory_from_key(key)
            value = self.__save_to_redis__(key, value)
        super().__setitem__(key, value)
        return value

    def __setitem__(self, key, value: dict):
        key = str(key)
        value = self.__save_to_redis__(key, value)
        super().__setitem__(key, value)

    def __delitem__(self, key):
        self.__remove_from_redis__(key)
        super().__delitem__(key)

    def __iter__(self):
        return iter(self.keys() | self.__read_keys_from_redis__())


class RedisDictStore(BaseRedisStore):
    """
        Dictionary that store many dicts, every by his own key in Redis
        Every dict is RedisDict - dict that store as solid json
        It's used 'lazy read' from Redis, only when key is requested
    """

    def default_factory_from_key(self, key):
        return dict()

    def __read_from_redis__(self, key):
        return RedisDict(self._redis, self.key2id(key))

    def __save_to_redis__(self, key, value: dict):
        if not isinstance(value, RedisDict):
            assert isinstance(value, dict), f'item value of RedisDictStore must be a dict, not {type(value)}'
            value = RedisDict(self._redis, self.key2id(key), value.items())
        value.flush()
        return value

    def flush(self):
        for value in self.values():
            value.flush()


class RedisSimpleStore(BaseRedisStore):
    """
        Dictionary that store many values
        Every value is stored as json by his own key in Redis
        When init - it's immediate read every keys from Redis occurred
        All keys convert to json, so when reverse decoding int -> int, str -> str, tuple -> list -> tuple
    """

    def key2id(self, key):
        return f"{self.id}:{json.dumps(key)}"

    def id2key(self, id):
        key = json.loads(id[len(self.id) + 1:])
        if isinstance(key, list):
            key = tuple(key)
        return key
