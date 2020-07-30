namespace RedisShardingBundle
{
    using Firehose.Cache.Distributed.Redis;
    using StackExchange.Redis;
    using System;
    using System.Threading;

    public sealed class RedisConnectionsManager
    {
        public int FixedApplicationShards { get; }

        public int OldClustersCount { get; }

        public int NewClustersCount { get; }

        public RedisConnectionConfig[] RedisConnectionConfigs { get; }

        public int CutOverWindowInMinutes { get; set; }

        private readonly PartitionCalculator partitionCalculator;

        public RedisConnectionsManager(int totalShards, int oldClustersCount, int newClustersCount, int cutOverWindowInMinutes, RedisConnectionConfig[] redisConnectionConfigs, CancellationToken cancellationToken)
        {
            this.FixedApplicationShards = totalShards;
            this.OldClustersCount = oldClustersCount;
            this.NewClustersCount = newClustersCount;
            this.CutOverWindowInMinutes = cutOverWindowInMinutes;
            if (newClustersCount > redisConnectionConfigs.Length)
            {
                throw new IndexOutOfRangeException("Cluster size and configurations don't match");
            }
            this.RedisConnectionConfigs = redisConnectionConfigs;
            partitionCalculator = new PartitionCalculator(oldClustersCount, newClustersCount, totalShards, CutOverWindowInMinutes, GetDatabase(0), cancellationToken);
        }

        public RedisValue StringGet(string partitionKey, string redisKey)
        {
            int clusterId = partitionCalculator.CalculateReadPartition(partitionKey);
            IDatabase database = GetDatabase(clusterId);
            return database.StringGet(redisKey);
        }
        
        public bool StringSet(string partitionKey, RedisKey redisKey, RedisValue redisValue, TimeSpan ? timeSpan = null)
        {
            bool response = true;
            int[] clusterIds = partitionCalculator.CalculateWritePartitions(partitionKey);
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
            int[] clusterIds = partitionCalculator.CalculateWritePartitions(partitionKey);
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
            int[] clusterIds = partitionCalculator.CalculateWritePartitions(partitionKey);
            foreach (int clusterId in clusterIds)
            {
                IDatabase database = GetDatabase(clusterId);
                response = response && database.KeyDelete(redisKey, flags);
            }
            return response;
        }

        public RedisValue[] HashGet(string partitionKey, RedisKey redisKey, RedisValue[] redisValues, CommandFlags flags = CommandFlags.None)
        {
            int clusterId = partitionCalculator.CalculateReadPartition(partitionKey);
            IDatabase database = GetDatabase(clusterId);
            return database.HashGet(redisKey, redisValues, flags);
        }

        public bool HashSet(string partitionKey, RedisKey redisKey, HashEntry[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            int[] clusterIds = partitionCalculator.CalculateWritePartitions(partitionKey);
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
