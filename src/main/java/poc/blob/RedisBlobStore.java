package poc.blob;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
  */
public class RedisBlobStore {

    private static final int TTL = 10*60;//10 minutes

    private static JedisPool jedisPool;

    public static void init(String address, String key) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(Integer.MAX_VALUE);
        poolConfig.setTestOnBorrow(true);

        jedisPool = new JedisPool(poolConfig, address, 6379, 0, key);
    }


    public static void put(String key, String blob) {
        jedisPool.getResource().setex(key, TTL, blob);
    }


    public static String get(String key) {
        return jedisPool.getResource().get(key);
    }

}
