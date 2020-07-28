using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
namespace RedisShardingBundle
{
    internal class PartitionCalculator
    {
        private readonly bool CutOver;

        private readonly SHA256 Sha256;

        private readonly int OldClusterCount;

        private readonly int NewClusterCount;

        private readonly int MaxPossibleShards;

        private readonly long CutOverTimestampInHours;

        public PartitionCalculator(int oldClusterCount, int newClusterCount, int maxPossibleShards, long cutOverTimestampInHours, IDatabase database)
        {
            OldClusterCount = oldClusterCount;
            NewClusterCount = newClusterCount;
            MaxPossibleShards = maxPossibleShards;
            Sha256 = SHA256.Create();
            CutOverTimestampInHours = cutOverTimestampInHours;
            CutOver = DataMigrationSetUpPrerequisitesAsync(database).Result;
        }

        private async Task<bool> DataMigrationSetUpPrerequisitesAsync(IDatabase database)
        {
            DataMigrationMeta dataMigrationMeta = await FetchDataMigrationStatusFromRedis(database);
            if(null != dataMigrationMeta && dataMigrationMeta.OldClusterCount != NewClusterCount)
            {
                if(dataMigrationMeta.NewClusterCount != NewClusterCount)
                {
                    //first machine to trigger the cluser migration
                    await AddOrUpdateDataMigrationMeta(database, false);
                }
                // else cluster migration has already been triggered and these are follow-up instances coming up
                //these dont need to update anything on the cache.
                return false;
            }
            if(null == dataMigrationMeta)
            {
                //add default configurations to redis
                //always store data in the oldest cluster.
                await AddOrUpdateDataMigrationMeta(database, true);
            }
            return true;
        }

        private async Task<DataMigrationMeta> FetchDataMigrationStatusFromRedis(IDatabase database)
        {
            HashEntry[] hashEntries = await database.HashGetAllAsync("DataMigrationMeta");
            return convertEntriesToMeta(hashEntries);
        }

        public int calculateReadPartition(string partitionKey)
        {
            int hashOfKey = CalculateHash(partitionKey);

            //application level shard.
            int partitionId = hashOfKey % MaxPossibleShards;

            return getClusterId(partitionId);
        }

        public int[] calculateWritePartitions(string partitionKey)
        {
            int hashOfKey = CalculateHash(partitionKey);
            int partitionId = hashOfKey % MaxPossibleShards;

            return GetWriteClusterIds(partitionId);
        }

        private int getClusterId(int partitionId)
        {
            int divisor = CutOver ? NewClusterCount : OldClusterCount;
            return GetClusterId(partitionId, divisor);
        }

        private int GetClusterId(int partitionId, int clusterCount)
        {
            return (partitionId * clusterCount) / MaxPossibleShards;
        }

        private int[] GetWriteClusterIds(int partitionId)
        {
            int[] clusterIds = CutOver ? new int[1] : new int[2];
            clusterIds[0] = GetClusterId(partitionId, NewClusterCount);
            if (!CutOver)
            {
                clusterIds[1] = GetClusterId(partitionId, OldClusterCount);
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

        //TODO:: code needs proper refactoring
        private DataMigrationMeta convertEntriesToMeta(HashEntry[] hashEntries)
        {
            if (null == hashEntries || hashEntries.Length == 0)
            {
                return null;
            }
            IDictionary<string, object> dictionary = new Dictionary<string, object>();
            foreach (HashEntry hashEntry in hashEntries)
            {
                dictionary.Add(hashEntry.Name, hashEntry.Value);
            }
            DataMigrationMeta dataMigrationMeta = new DataMigrationMeta()
            {
                OldClusterCount = (int)GetDataFromDictionary(dictionary, "OldClusterCount"),
                NewClusterCount = (int)GetDataFromDictionary(dictionary, "NewClusterCount"),
                CutOver = (bool)GetDataFromDictionary(dictionary, "CutOver"),
                CutOverTimestampInMillis = (long)GetDataFromDictionary(dictionary, "CutOverTimestampInMillis")
            };
            return dataMigrationMeta;
        }

        private object GetDataFromDictionary(IDictionary<string, object> dictionary, string key)
        {
            if (dictionary.TryGetValue("oldClusterCount", out object value))
            {
                return value;
            }
            return null;
        }

        private async Task AddOrUpdateDataMigrationMeta(IDatabase database, bool cutOver)
        {
            IList<HashEntry> hashEntries = new List<HashEntry>();
            hashEntries.Add(new HashEntry("OldClusterCount", OldClusterCount));
            hashEntries.Add(new HashEntry("NewClusterCount", NewClusterCount));
            hashEntries.Add(new HashEntry("CutOver", cutOver));
            //if cutOver has happened or cutOver is not required, reset the cutovertimestamp to -1
            //if cluster migration is triggered for the time, we set the CutOverTimeStamp
            hashEntries.Add(new HashEntry("CutOverTimestampInHours", cutOver ? -1 : CalculateCutOverTimestamp(CutOverTimestampInHours)));

            await database.HashSetAsync("DataMigrationMeta", hashEntries.ToArray());
        }

        private long CalculateCutOverTimestamp(long cutOverTimestampInHours)
        {
            TimeSpan t = DateTime.Now - new DateTime(1970, 1, 1);
            return (long)t.TotalMilliseconds + cutOverTimestampInHours * (60 * 60 * 1000);
        }
    }
}
