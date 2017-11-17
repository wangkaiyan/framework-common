package com.framework.redis;

import com.framework.util.SerializeUtil;
import org.apache.log4j.Logger;
import redis.clients.jedis.*;

import java.io.*;
import java.util.*;

/**
 * Created by wangkaiyan on 2017/11/17.
 */
public class ShardJedisClient {
    private static final Logger log = Logger.getLogger(ShardJedisClient.class);
    // redis的配置文件名字，默认redis.properties
    private  String config_path="";
    private final Properties properties = new Properties();
    // shardedJedis池
    private ShardedJedisPool shardPool;

    private ShardJedisClient(){}

    static final String REDIS_RET_OK = "OK";
    // 应用程序根据指定的redis配置文件来初始化池配置
    public ShardJedisClient(String config_name) throws FileNotFoundException, IOException {
        this.config_path =config_name;
        if(shardPool==null){
            initial();
        }
    }

    private void initial() throws FileNotFoundException, IOException {
        File file = new File(config_path);
        Reader reader = null;
        if(file.exists()) {
            try {
                reader = new FileReader(config_path);
            } catch (FileNotFoundException e) {
                reader = new InputStreamReader(ShardJedisClient.class.getResourceAsStream("/"+config_path));
            }
        }else{
            reader = new InputStreamReader(ShardJedisClient.class.getResourceAsStream("/"+config_path));
        }

        // 加载redis配置文件
        properties.load(reader);
        // 创建jedis池配置实例
        JedisPoolConfig config = new JedisPoolConfig();
        // 设置池配置项值
//        config.setMaxActive(Integer.valueOf(properties.getProperty("redis.pool.maxActive")));
        config.setMaxIdle(Integer.valueOf(properties.getProperty("redis.pool.maxIdle")));
        config.setMaxWaitMillis(Long.valueOf(properties.getProperty("redis.pool.maxWait")));
        config.setTestOnBorrow(Boolean.valueOf(properties.getProperty("redis.pool.testOnBorrow")));
        config.setTestOnReturn(Boolean.valueOf(properties.getProperty("redis.pool.testOnReturn")));

        // 根据配置创建多个redis共享服务
        int redis_num = Integer.valueOf(properties.getProperty("redis.num"));
        List<JedisShardInfo> list = new LinkedList<JedisShardInfo>();
        if (redis_num > 0) {
            for (int i = 0; i < redis_num; i++) {
                JedisShardInfo jedisShardInfo = new JedisShardInfo(properties.getProperty("redis" + i + ".ip"), Integer.valueOf(properties.getProperty("redis" + i + ".port")));
                list.add(jedisShardInfo);
            }
        }

        // 根据配置文件,创建shared池实例
        shardPool = new ShardedJedisPool(config, list);
    }


    
    public boolean expire(String key, int seconds) {
        ShardedJedis shardJedis = shardPool.getResource();
        Long result = null;
        try {
            result = shardJedis.expire(key, seconds);
        } catch (Exception e) {
            shardPool.returnBrokenResource(shardJedis);
            log.error("redis expire error!"+e);
            return false;
        }
        shardPool.returnResource(shardJedis);
        if (result == null || result != 1) {
            return false;
        }
        return true;
    }

    
    public Long del(final String key){
        ShardedJedis shardJedis = shardPool.getResource();
        Long result = null;
        try {
            result = shardJedis.del(key);
        } catch (Exception e) {
            shardPool.returnBrokenResource(shardJedis);
            log.error("redis expire error!"+e);
            return result;
        }
        shardPool.returnResource(shardJedis);
        return result;
    }

    
    public Long setnx(String key, String value) {
        ShardedJedis shardJedis = shardPool.getResource();
        Long result = null;
        try {
            result = shardJedis.setnx(key, value);
        } catch (Exception e) {
            shardPool.returnBrokenResource(shardJedis);
            log.error("redis expire error!"+e);
            return result;
        }
        shardPool.returnResource(shardJedis);
        return result;
    }


    public Boolean set(byte[] key, byte[] value) {
        ShardedJedis shardJedis = shardPool.getResource();
        String result = null;
        try {
            result = shardJedis.set(key, value);
        } catch (Exception e) {
            shardPool.returnBrokenResource(shardJedis);
            log.error("redis expire error!"+e);
            return false;
        }
        shardPool.returnResource(shardJedis);
        return REDIS_RET_OK.equalsIgnoreCase(result);
    }


    public Boolean set(String key, String value) {
        ShardedJedis shardJedis = shardPool.getResource();
        String result = null;
        try {
            result = shardJedis.set(key, value);
        } catch (Exception e) {
            shardPool.returnBrokenResource(shardJedis);
            log.error("redis expire error!"+e);
            return false;
        }
        shardPool.returnResource(shardJedis);
        return REDIS_RET_OK.equalsIgnoreCase(result);
    }


     
    public String get(String key) {
        ShardedJedis shardJedis = shardPool.getResource();
        String result = null;
        try {
            result = shardJedis.get(key);
        } catch (Exception e) {
            shardPool.returnBrokenResource(shardJedis);
            log.error("redis expire error!"+e);
            return result;
        }
        shardPool.returnResource(shardJedis);
        return result;
    }



    public Long rpush(byte[] key, byte[] value) {
        ShardedJedis shardJedis = shardPool.getResource();
        Long result = null;
        try {
            result = shardJedis.rpush(key, value);
        } catch (Exception e) {
            shardPool.returnBrokenResource(shardJedis);
            log.error("redis expire error!"+e);
            return result;
        }
        shardPool.returnResource(shardJedis);
        return result;
    }


     
    public Long rpush(String key, String value) {
        ShardedJedis shardJedis = shardPool.getResource();
        Long result = null;
        try {
            result = shardJedis.rpush(key, value);
        } catch (Exception e) {
            shardPool.returnBrokenResource(shardJedis);
            log.error("redis expire error!"+e);
            return result;
        }
        shardPool.returnResource(shardJedis);
        return result;
    }


    public String rpop(String key) {
        ShardedJedis shardJedis = shardPool.getResource();
        String result = null;
        try {
            result = shardJedis.rpop(key);
        } catch (Exception e) {
            shardPool.returnBrokenResource(shardJedis);
            log.error("redis expire error!"+e);
            return result;
        }
        shardPool.returnResource(shardJedis);
        return result;
    }


    public String lpop(String key) {
        ShardedJedis shardJedis = shardPool.getResource();
        String result = null;
        try {
            result = shardJedis.lpop(key);
        } catch (Exception e) {
            shardPool.returnBrokenResource(shardJedis);
            log.error("redis expire error!"+e);
            return result;
        }
        shardPool.returnResource(shardJedis);
        return result;
    }


     
    public Long lpush(byte[] key, byte[] value) {
        ShardedJedis shardJedis = shardPool.getResource();
        Long result = null;
        try {
            result = shardJedis.lpush(key, value);
        } catch (Exception e) {
            shardPool.returnBrokenResource(shardJedis);
            log.error("redis expire error!"+e);
            return result;
        }
        shardPool.returnResource(shardJedis);
        return result;
    }


    public Long lpush(String key, String value) {
        ShardedJedis shardJedis = shardPool.getResource();
        Long result = null;
        try {
            result = shardJedis.lpush(key, value);
        } catch (Exception e) {
            shardPool.returnBrokenResource(shardJedis);
            log.error("redis expire error!"+e);
            return result;
        }
        shardPool.returnResource(shardJedis);
        return result;
    }


    public byte[] get(byte[] key) {
        ShardedJedis shardJedis = shardPool.getResource();
        byte[] result = null;
        try {
            result = shardJedis.get(key);
        } catch (Exception e) {
            shardPool.returnBrokenResource(shardJedis);
            log.error("redis get error!"+e);
            return result;
        }
        shardPool.returnResource(shardJedis);
        return result;
    }

    public <T extends Serializable> List<T> hvalsToObject(String key){
        List<T> list= new ArrayList<T>();
        ShardedJedis shardJedis = shardPool.getResource();
        Collection<byte[]> result = null;
        try {
            result = shardJedis.hvals(key.getBytes());
        } catch (Exception e) {
            shardPool.returnBrokenResource(shardJedis);
            log.error("redis hvalsToObject error!"+e);
            return null;
        }
        shardPool.returnResource(shardJedis);
        Iterator<byte[]> it= result.iterator();
        while(it.hasNext()){
            list.add((T) SerializeUtil.decode(it.next()));
        }
        return list;
    }


    public ShardedJedisPipeline getPipeline(){
        ShardedJedis shardJedis = shardPool.getResource();
        ShardedJedisPipeline pipeline  = shardJedis.pipelined();
        return pipeline;
    }
}
