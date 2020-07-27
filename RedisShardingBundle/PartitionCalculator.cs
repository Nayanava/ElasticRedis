using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace RedisShardingBundle
{
    internal class PartitionCalculator
    {
        private bool cutOver = false;

        private SHA256 sha256;

        private int OldClusterCount;

        private int NewClusterCount;

        private int MaxPossibleShards;

        private PartitionCalculator instance;
        public PartitionCalculator(int oldClusterCount, int newClusterCount, int maxPossibleShards)
        {
            OldClusterCount = oldClusterCount;
            NewClusterCount = newClusterCount;
            MaxPossibleShards = maxPossibleShards;
        }

        public int calculateReadPartition(string partitionKey)
        {
            int hashOfKey = calculateHash(partitionKey);

            //application level shard.
            int shardId = hashOfKey % MaxPossibleShards;

            return getClusterId(shardId);
        }

        public int[] calculateWritePartitions(string partitionKey)
        {
            int hashOfKey = calculateHash(partitionKey);
            int shardId = hashOfKey % MaxPossibleShards;

            return getWriteClusterIds(shardId);
        }

        private int getClusterId(int shardId)
        {
            int divisor = cutOver ? NewClusterCount : OldClusterCount;
            return getClusterId(shardId, divisor);
        }

        private int getClusterId(int shardId, int clusterCount)
        {
            return shardId % (MaxPossibleShards / clusterCount);
        }

        private int[] getWriteClusterIds(int shardId)
        {
            int[] clusterIds = cutOver ? new int[1] : new int[2];
            clusterIds[0] = getClusterId(shardId, NewClusterCount);
            if (!cutOver)
            {
                clusterIds[1] = getClusterId(shardId, OldClusterCount);
            }

            return clusterIds;
        }

        private int calculateHash(string partitionKey)
        {
            var keyAsByteArray = Encoding.UTF8.GetBytes(partitionKey);
            var hashAsArray = sha256.ComputeHash(keyAsByteArray);
            return BitConverter.ToInt32(hashAsArray, 0);
        }
    }
}
