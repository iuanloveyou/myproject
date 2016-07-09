package redis;


import junit.framework.TestCase;
import redis.util.JedisUtil;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

/**
 * Created by re on 2016/7/8.
 */
public class RedisTest extends TestCase {

    public void testWrite(){
        ShardedJedis jds = JedisUtil.getJedis();
        jds.set("my", "try");
        jds.expire("my", 1000);
        jds.close();
        ShardedJedisPipeline pipe = jds.pipelined();
    }
}
