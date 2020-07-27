namespace RedisShardingBundle
{
    using Firehose.Cache.Distributed.Redis;
    using StackExchange.Redis;
    using System;

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
            partitionCalculator = new PartitionCalculator(oldClustersCount, newClustersCount, totalShards);
        }

        public RedisValue StringGet(string partitionKey, string redisKey)
        {
            int clusterId = partitionCalculator.calculateReadPartition(partitionKey);
            IDatabase database = GetDatabase(clusterId);
            return database.StringGet(redisKey);
        }
        
        public bool StringSet(string partitionKey, RedisKey redisKey, RedisValue redisValue, TimeSpan timeSpan)
        {
            bool response = true;
            int[] clusterIds = partitionCalculator.calculateWritePartitions(partitionKey);
            foreach(int clusterId in clusterIds) 
            {
                IDatabase database = GetDatabase(clusterId);
                response = response && database.StringSet(redisKey, redisValue, timeSpan);
            }
            return response;
        }

        public bool KeyExpire(string partitionKey, RedisKey redisKey, TimeSpan timeSpan, CommandFlags flags = CommandFlags.None)
        {
            bool response = true;
            int[] clusterIds = partitionCalculator.calculateWritePartitions(partitionKey);
            foreach(int clusterId in clusterIds)
            {
                IDatabase database = GetDatabase(clusterId);
                response = response && database.KeyExpire(redisKey, timeSpan, flags);
            }
            return response;
        }

        public bool KeyDelete(string partitionKey, RedisKey redisKey, CommandFlags flags = CommandFlags.None)
        {
            bool response = true;
            int[] clusterIds = partitionCalculator.calculateWritePartitions(partitionKey);
            foreach (int clusterId in clusterIds)
            {
                IDatabase database = GetDatabase(clusterId);
                response = response && database.KeyDelete(redisKey, flags);
            }
            return response;
        }

        public RedisValue[] HashGet(string partitionKey, RedisKey redisKey, RedisValue[] redisValues, CommandFlags flags = CommandFlags.None)
        {
            int clusterId = partitionCalculator.calculateReadPartition(partitionKey);
            IDatabase database = GetDatabase(clusterId);
            return database.HashGet(redisKey, redisValues, flags);
        }

        public bool HashSet(string partitionKey, RedisKey redisKey, HashEntry[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            int[] clusterIds = partitionCalculator.calculateWritePartitions(partitionKey);
            foreach (int clusterId in clusterIds)
            {
                IDatabase database = GetDatabase(clusterId);
                database.HashSet(redisKey, hashFields, flags);
            }
            return true;
        }
        private IDatabase GetDatabase(int clusterId)
        {
            return RedisConnectionConfigs[clusterId].GetRedisDatabase();
        }
    }
}
