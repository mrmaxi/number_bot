import json
from redis import StrictRedis
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


class RedisDictStore(dict):
    """
        Dictionary that store many dicts, every by his own key in Redis
        Every dict is RedisDict - dict that store as solid json
        It's used 'lazy read' from Redis, only when key is requested
    """

    _redis = None
    id = None

    def key2id(self, key):
        return f"{self.id}:{key}"

    def id2key(self, id):
        return id[len(self.id) + 1:]

    def __read_from_redis__(self, key):
        return RedisDict(self._redis, self.key2id(key))

    def __read_keys_from_redis__(self):
        return [self.id2key(id) for id in self._redis.keys(f'{self.id}:*')]

    def __init__(self, redis_url, id, lazy_read=True):
        self.id = id
        self._redis = StrictRedis.from_url(redis_url, decode_responses=True)
        args = []
        if not lazy_read:
            args = [(key, self.__read_from_redis__(key)) for key in self.__read_keys_from_redis__()]
        super().__init__(*args)

    def __missing__(self, key):
        key = str(key)
        logger.debug(f'check {key} in redis')
        value = self.__read_from_redis__(key)
        if value:
            logger.debug(f'read {key} from redis = {value}')
        super().__setitem__(key, value)
        return value

    def __setitem__(self, key, value):
        key = str(key)
        if not isinstance(value, RedisDict):
            assert isinstance(value, dict), f'item value of RedisDictStore must be a dict, not {type(value)}'
            value = RedisDict(self._redis, self.key2id(key), value.items())
            value.flush()
        super().__setitem__(key, value)

    def __iter__(self):
        return iter(self.keys() | self.__read_keys_from_redis__())


class RedisSimpleStore(dict):
    """
        Dictionary that store many values
        Every value is stored as json by his own key in Redis
        When init - it's immediate read every keys from Redis occurred
    """

    _redis = None
    id = None

    def key2id(self, key):
        return f"{self.id}:{json.dumps(key)}"

    def id2key(self, id):
        key = json.loads(id[len(self.id) + 1:])
        if isinstance(key, list):
            key = tuple(key)
        return key

    def __init__(self, redis_url, id):
        self.id = id
        self._redis = StrictRedis.from_url(redis_url, decode_responses=True)
        ids = self._redis.keys(self.id + ':*')
        keys = [self.id2key(key_id) for key_id in ids]
        super().__init__(zip(keys, map(json.loads, map(self._redis.get, ids))))

    def __delitem__(self, key):
        id = self.key2id(key)
        self._redis.delete(id)
        super().__delitem__(key)

    def __setitem__(self, key, value):
        id = self.key2id(key)
        if not callable(value):
            try:
                encoded_value = json.dumps(value)
            except TypeError:
                pass
            else:
                self._redis.set(id, encoded_value)

        super().__setitem__(key, value)