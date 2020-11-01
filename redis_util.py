import json
from redis import StrictRedis
import logging

logger = logging.getLogger(__name__)


class RedisDict(dict):
    """
        Dictionary, that store in Redis as one solid json by his own key
        for save dict in redis it's need to call .save() method
        when __init__ called read object from redis occurred by default
            Redis object will be overridden if 'source' argument passed
    """

    _redis = None
    id = None

    def __init__(self, redis, id, source=None):
        super().__init__()
        self._redis = redis
        self.id = id
        if source:
            print(source)
            self.update(source)
            self.save()
        else:
            self.read()

    def read(self):
        self.clear()
        self.update(json.loads(self._redis.get(self.id) or '{}'))

    def save(self):
        obj = {k: v for k, v in self.items() if not callable(v)}
        self._redis.set(self.id, json.dumps(obj))


class RedisDictStore(dict):
    """
        Dictionary that store many dicts, every by his own key in Redis
        Every dict is RedisDict - dict that store as solid json
        It's used 'lazy read' from Redis, only when key is requested
    """

    _redis = None
    id = None

    def __init__(self, redis_url, id):
        super().__init__()
        self._redis = StrictRedis.from_url(redis_url, decode_responses=True)
        self.id = id

    def __missing__(self, key):
        id = self.id + ':' + str(key)
        logger.debug(f'check {key} in redis')
        value = RedisDict(self._redis, id)
        if value:
            logger.debug(f'read {key} from redis = {value}')
            super().__setitem__(key, value)
        return value

    def __setitem__(self, key, value):
        if not isinstance(value, RedisDict):
            id = self.id+':'+str(key)
            value = RedisDict(self._redis, id, value)
        super().__setitem__(key, value)

    def __iter__(self):
        return iter([key[len(self.id)+1:] for key in self._redis.keys(self.id + ':*')])


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
        super().__init__()
        self._redis = StrictRedis.from_url(redis_url, decode_responses=True)
        self.id = id
        ids = self._redis.keys(self.id + ':*')
        keys = [self.id2key(key_id) for key_id in ids]
        super().update(zip(keys, map(json.loads, map(self._redis.get, ids))))

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
                None
            else:
                self._redis.set(id, encoded_value)

        super().__setitem__(key, value)