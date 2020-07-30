using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
namespace RedisShardingBundle
{
    internal class PartitionCalculator
    {
        private bool CutOver;

        private readonly SHA256 Sha256;

        private readonly int OldClusterCount;

        private readonly int NewClusterCount;

        private readonly int MaxPossibleShards;

        private readonly long CutOverTimestampInMinutes;

        private readonly CancellationToken Token;

        public PartitionCalculator(int oldClusterCount, int newClusterCount, int maxPossibleShards, long cutOverWindowInMinutes, IDatabase database, CancellationToken cancellationToken)
        {
            OldClusterCount = oldClusterCount;
            NewClusterCount = newClusterCount;
            MaxPossibleShards = maxPossibleShards;
            Token = cancellationToken; 
            Sha256 = SHA256.Create();
            CutOverTimestampInMinutes = cutOverWindowInMinutes;
            DataMigrationSetUpPrerequisitesAsync(database).Await(HandleException);
        }


        private async Task DataMigrationSetUpPrerequisitesAsync(IDatabase database)
        {
            DataMigrationMeta dataMigrationMeta = await FetchDataMigrationStatusFromRedis(database);
            if (null == dataMigrationMeta)
            {
                //add default configurations to redis
                //always store data in the oldest cluster.
                this.CutOver = await AddOrUpdateDataMigrationMetaAsync(database, true);
                return;
            }
            if (dataMigrationMeta.OldClusterCount != NewClusterCount)
            {
                if (dataMigrationMeta.NewClusterCount != NewClusterCount)
                {
                    //first machine to trigger the cluser migration
                    await AddOrUpdateDataMigrationMetaAsync(database, false);
                }
                // else cluster migration has already been triggered and these are follow-up instances coming up
                //these dont need to update anything on the cache.
                //configures the machine for future CutOver refresh
                await RefreshCutOverFlag(database);
            }
            this.CutOver = true;
        }

        private async Task<DataMigrationMeta> FetchDataMigrationStatusFromRedis(IDatabase database)
        {
            HashEntry[] hashEntries = await database.HashGetAllAsync("DataMigrationMeta");
            return convertEntriesToMeta(hashEntries);
        }

        public int CalculateReadPartition(string partitionKey)
        {
            int hashOfKey = CalculateHash(partitionKey);

            //application level shard.
            int partitionId = ((hashOfKey % MaxPossibleShards) + MaxPossibleShards) % MaxPossibleShards;

            return getClusterId(partitionId);
        }

        public int[] CalculateWritePartitions(string partitionKey)
        {
            int hashOfKey = CalculateHash(partitionKey);
            int partitionId = ((hashOfKey % MaxPossibleShards) + MaxPossibleShards) % MaxPossibleShards;

            return GetWriteClusterIds(partitionId);
        }

        private int getClusterId(int partitionId)
        {
            int divisor = CutOver ? NewClusterCount : OldClusterCount;
            string logLine = CutOver ? "Migration Complete, Reads enabled on new Cluster" : "Migration In Progress, Reads Enabled on old Cluster";
            logLine += $" PartitionId : {partitionId}";
            Console.WriteLine(logLine);
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
            string logLine = !CutOver ? $"Data Migration In progress, Writes enabled on old cluster : {clusterIds[1]}, new cluster : {clusterIds[0]}"
                : $"Data Migration Complete, Writes enabled only on new Cluster : {clusterIds[0]}";
            logLine += $" PartitionId : {partitionId}";
            Console.WriteLine(logLine);
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
                OldClusterCount = int.Parse(GetDataFromDictionary(dictionary, "OldClusterCount")),
                NewClusterCount = int.Parse(GetDataFromDictionary(dictionary, "NewClusterCount")),
                CutOver = (int.Parse(GetDataFromDictionary(dictionary, "CutOver")) == 1) ? true : false,
                CutOverTimestampInMillis = long.Parse(GetDataFromDictionary(dictionary, "CutOverTimestampInMillis"))
            };
            return dataMigrationMeta;
        }

        private string GetDataFromDictionary(IDictionary<string, object> dictionary, string key)
        {
            if (dictionary.TryGetValue(key, out object value))
            {
                return value.ToString();
            }
            return null;
        }

        private async Task<bool> AddOrUpdateDataMigrationMetaAsync(IDatabase database, bool cutOver)
        {
            IList<HashEntry> hashEntries = new List<HashEntry>();
            hashEntries.Add(new HashEntry("OldClusterCount", OldClusterCount));
            hashEntries.Add(new HashEntry("NewClusterCount", NewClusterCount));
            hashEntries.Add(new HashEntry("CutOver", cutOver));
            //if cutOver has happened or cutOver is not required, reset the cutovertimestamp to -1
            //if cluster migration is triggered for the time, we set the CutOverTimeStamp
            hashEntries.Add(new HashEntry("CutOverTimestampInMillis", cutOver ? -1 : CalculateCutOverTimestamp(CutOverTimestampInMinutes)));

            await database.HashSetAsync("DataMigrationMeta", hashEntries.ToArray());
            return cutOver;
        }

        private async Task AddOrUpdateDataMigrationMeta(IDatabase database, DataMigrationMeta dataMigrationMeta)
        {
            IList<HashEntry> hashEntries = new List<HashEntry>();
            hashEntries.Add(new HashEntry("OldClusterCount", dataMigrationMeta.NewClusterCount));
            hashEntries.Add(new HashEntry("NewClusterCount", dataMigrationMeta.NewClusterCount));
            hashEntries.Add(new HashEntry("CutOver", dataMigrationMeta.CutOver));
            hashEntries.Add(new HashEntry("CutOverTimestampInMillis", dataMigrationMeta.CutOverTimestampInMillis));

            await database.HashSetAsync("DataMigrationMeta", hashEntries.ToArray());
        }

        private long CalculateCutOverTimestamp(long cutOverWindowInMinutes)
        {
            return CurrentTimeInMilliSeconds() + (cutOverWindowInMinutes * (60 * 1000));
        }

        private long CurrentTimeInMilliSeconds()
        {
            TimeSpan t = DateTime.Now - new DateTime(1970, 1, 1);
            return (long)t.TotalMilliseconds;
        }

        private async Task<bool> UpdateOnCutOverCompleteAsync(IDatabase database, DataMigrationMeta dataMigrationMeta)
        {
            if (CurrentTimeInMilliSeconds() > dataMigrationMeta.CutOverTimestampInMillis)
            {
                dataMigrationMeta.CutOver = true;
                dataMigrationMeta.CutOverTimestampInMillis = 0;
                await AddOrUpdateDataMigrationMeta(database, dataMigrationMeta);
            }
            return dataMigrationMeta.CutOver;
        }
        private async Task RefreshCutOverFlag(IDatabase database)
        {
            while(!Token.IsCancellationRequested)
            {
                DataMigrationMeta dataMigrationMeta = await FetchDataMigrationStatusFromRedis(database);
                //if the flag is turned off, but the CutOver time has crossed, update the settings
                if (null != dataMigrationMeta && !dataMigrationMeta.CutOver)
                {
                    CutOver = await UpdateOnCutOverCompleteAsync(database, dataMigrationMeta);
                }
                int delayPeriod = CutOver ? -1 : (int)(dataMigrationMeta.CutOverTimestampInMillis - CurrentTimeInMilliSeconds());

                await Task.Delay(TimeSpan.FromMilliseconds(delayPeriod), Token);
            }
        }

        private void HandleException( Exception ex)
        {
            throw ex;
        }
    }
    public static class AwaitOnTask
    {
        public static async void Await<T>(this Task<T> task, Action<T> completed, Action<Exception> errorCallBack)
        {
            try
            {
               T result = await task;
                completed?.Invoke(result);
            }
            catch (Exception ex)
            {
                errorCallBack?.Invoke(ex);
            }
        }

        public static async void Await(this Task task, Action<Exception> errorCallBack)
        {
            try
            {
                await task;
            } catch(Exception ex)
            {
                errorCallBack?.Invoke(ex);
            }
        }
    }
}
