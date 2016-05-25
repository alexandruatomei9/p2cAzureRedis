package poc.blob;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
  */
public class RedisBlobStore {

    private static final int TTL = 10*60;//10 minutes

    private static JedisPool jedisPool;

    private static Jedis jedis;

    public static void init(String address, String key) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(Integer.MAX_VALUE);
        poolConfig.setTestOnBorrow(true);

        jedisPool = new JedisPool(poolConfig, address, 6379, 0, key);
    }

    public static void init2(JedisPool jedisPool) {
        RedisBlobStore.jedisPool = jedisPool;
        jedis = jedisPool.getResource();
    }

    public static void put(String key, String blob) {
//        try (Jedis jedis = jedisPool.getResource()) {
//            jedis.setex(key, TTL, blob);
//        }
        jedis.setex(key, TTL, blob);
    }


    public static String get(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        }
    }

}
