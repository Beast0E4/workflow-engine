package com.enginecorp.workfloworchestrator.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {

    @Value("${redisson.address}")
    private String redisAddress;

    @Value("${redisson.database}")
    private int redisDatabase;

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient() {
        Config redissonConfig = new Config();

        redissonConfig.useSingleServer()
            .setAddress(redisAddress)
            .setDatabase(redisDatabase)
            .setConnectionPoolSize(20)
            .setConnectionMinimumIdleSize(5);

        return Redisson.create(redissonConfig);
    }
}