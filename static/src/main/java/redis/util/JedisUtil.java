package redis.util;

import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by iuan on 2016/7/8.
 */
public class JedisUtil {

    private static ShardedJedis jds = null;
    private static ShardedJedisPool pool = null;

    private static ShardedJedisPool getPool() {
        if (pool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(RedisProperty.maxActive);
            config.setMaxIdle(RedisProperty.maxIdle);
            config.setMaxWaitMillis(RedisProperty.maxIdle);
            config.setTestOnBorrow(true);
            config.setTestOnReturn(true);
            try {
                List<JedisShardInfo> list = new ArrayList<JedisShardInfo>();
                String[] Clients = RedisProperty.addr.split(";");
                for (String str : Clients) {
                    String[] hostport= str.split(":");
                    JedisShardInfo info = new JedisShardInfo(hostport[0], Integer.parseInt(hostport[1]));
                    list.add(info);
                }
                pool = new ShardedJedisPool(config, list);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
        return pool;
    }

    public static ShardedJedis getJedis() {
        try {
            jds = JedisUtil.getPool().getResource();
            return jds;
        } catch (Exception e) {
            e.printStackTrace();
            if (jds != null) {
                jds.close();
            }
            return null;
        }
    }

}