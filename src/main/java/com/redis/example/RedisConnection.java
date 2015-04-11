package com.redis.example;

import redis.clients.jedis.Jedis;

/**
 * Created by Krishna on 11/04/15.
 */
public class RedisConnection {
    public static void main(String args[])
    {
        Jedis jedis = new Jedis("localhost");
        System.out.println("Connected Sucessfully");
        jedis.set("test","Successful");
    }
}
