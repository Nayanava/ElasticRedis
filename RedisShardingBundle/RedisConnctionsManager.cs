using Firehose.Cache.Distributed.Redis;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace RedisShardingBundle
{
    public sealed class RedisConnectionsManager
    {
        public int TotalShards { get; }

        public int OldClustersCount { get; }

        public int NewClustersCount { get; }

        public RedisConnectionConfig[] RedisConnectionConfigs { get; }

        public RedisConnectionsManager(int totalShards, int oldClustersCount, int newClustersCount, RedisConnectionConfig[] redisConnectionConfigs)
        {
            this.TotalShards = totalShards;
            this.OldClustersCount = oldClustersCount;
            this.NewClustersCount = newClustersCount;
            if (newClustersCount != redisConnectionConfigs.Length)
            {
                throw new IndexOutOfRangeException("Cluster size and configurations don't match");
            }
            this.RedisConnectionConfigs = redisConnectionConfigs;
        }

        public RedisValue get(String partitionKey, String searchKey)
        {
            int shardId = calculateShard(partitionKey);
            IDatabase database = RedisConnectionConfigs[shardId].GetRedisDatabase();
            return database.StringGet(searchKey);
        }

        private int calculateShard(string partitionKey)
        {
            throw new NotImplementedException();
        }
    }
}
