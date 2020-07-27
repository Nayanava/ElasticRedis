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

        public int CutOverWindowInHours { get; set; }

        private readonly PartitionCalculator partitionCalculator;

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
            PartitionCalculator partitionCalculator = new PartitionCalculator(oldClustersCount, newClustersCount, totalShards);
        }

        public RedisValue get(string partitionKey, string searchKey)
        {
            int clusterId = partitionCalculator.calculateReadPartition(partitionKey);
            IDatabase database = RedisConnectionConfigs[clusterId].GetRedisDatabase();
            return database.StringGet(searchKey);
        }
        
        public bool StringSet(string partitionKey, RedisKey itemKey, RedisValue redisValue, TimeSpan timeSpan)
        {
            bool response = true;
            int[] clusterIds = partitionCalculator.calculateWritePartitions(partitionKey);
            foreach(int clusterId in clusterIds) {
                IDatabase database = RedisConnectionConfigs[clusterId].GetRedisDatabase();
                response = response && database.StringSet(itemKey, redisValue, timeSpan);
            }
            return response;
        }
    }
}
