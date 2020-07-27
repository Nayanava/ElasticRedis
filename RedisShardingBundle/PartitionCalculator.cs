using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace RedisShardingBundle
{
    internal class PartitionCalculator
    {
        private readonly bool CutOver = false;

        private readonly SHA256 Sha256;

        private readonly int OldClusterCount;

        private readonly int NewClusterCount;

        private readonly int MaxPossibleShards;

        public PartitionCalculator(int oldClusterCount, int newClusterCount, int maxPossibleShards)
        {
            OldClusterCount = oldClusterCount;
            NewClusterCount = newClusterCount;
            MaxPossibleShards = maxPossibleShards;
            Sha256 = SHA256.Create();
        }

        public int calculateReadPartition(string partitionKey)
        {
            int hashOfKey = CalculateHash(partitionKey);

            //application level shard.
            int shardId = hashOfKey % MaxPossibleShards;

            return getClusterId(shardId);
        }

        public int[] calculateWritePartitions(string partitionKey)
        {
            int hashOfKey = CalculateHash(partitionKey);
            int shardId = hashOfKey % MaxPossibleShards;

            return GetWriteClusterIds(shardId);
        }

        private int getClusterId(int shardId)
        {
            int divisor = CutOver ? NewClusterCount : OldClusterCount;
            return GetClusterId(shardId, divisor);
        }

        private int GetClusterId(int shardId, int clusterCount)
        {
            return shardId % (MaxPossibleShards / clusterCount);
        }

        private int[] GetWriteClusterIds(int shardId)
        {
            int[] clusterIds = CutOver ? new int[1] : new int[2];
            clusterIds[0] = GetClusterId(shardId, NewClusterCount);
            if (!CutOver)
            {
                clusterIds[1] = GetClusterId(shardId, OldClusterCount);
            }

            return clusterIds;
        }

        //using SHA256 to calculate the hash
        private int CalculateHash(string partitionKey)
        {
            var keyAsByteArray = Encoding.UTF8.GetBytes(partitionKey);
            var hashAsArray = Sha256.ComputeHash(keyAsByteArray);
            return BitConverter.ToInt32(hashAsArray, 0);
        }
    }
}
