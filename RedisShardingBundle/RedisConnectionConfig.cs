//-----------------------------------------------------------------------
// <copyright file="RedisConnectionManager.cs" company="Microsoft">
//     Copyright © Microsoft Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Firehose.Cache.Distributed.Redis
{
    using System;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using StackExchange.Redis;

    /// <summary>
    /// Class for onnection pool of the Azure Redis Connections from this client
    /// </summary>
    public sealed class RedisConnectionConfig
    {
        /// <summary>
        /// Client id prefix for the connection
        /// </summary>
        private const string ClientIdPrefix = "SkypeSpaces_";

        /// <summary>
        /// The cache store count
        /// </summary>
        private long cacheStoreCount;

        /// <summary>
        /// Lazy initialization for pooled connection multiplexers
        /// </summary>
        private Lazy<ConnectionMultiplexer>[] lazyConnections;

        /// <summary>
        /// Initializes a new instance of the RedisConnectionManager class
        /// </summary>
        /// <param name="connectionString">Redis connection string</param>
        /// <param name="poolSize">Number of pooled Redis connection multiplexers</param>
        public RedisConnectionConfig(string connectionString, int poolSize)
        {
            this.cacheStoreCount = 0;
            this.lazyConnections = new Lazy<ConnectionMultiplexer>[poolSize];

            Parallel.For(
                0,
                poolSize,
                i =>
                {
                    var config = this.GetConfigurationOptions(connectionString, i);
                    this.lazyConnections[i] = new Lazy<ConnectionMultiplexer>(() =>
                    {
                        var redisConnectionMultiplexer = ConnectionMultiplexer.Connect(config);
                        redisConnectionMultiplexer.PreserveAsyncOrder = false;
                        return redisConnectionMultiplexer;
                    });
                });
        }

        /// <summary>
        /// Gets the connection multiplexer
        /// </summary>
        /// <returns>Connection multiplexer</returns>
        public IDatabase GetRedisDatabase()
        {
            // This is a round robin way of getting the connection on which a request will be executed
            // Locking the cacheStoreCount for parallel connections
            long cacheStoreIndex = Interlocked.Increment(ref this.cacheStoreCount) % this.lazyConnections.Length;
            var connectionMultiplexer = this.lazyConnections[cacheStoreIndex].Value;
            return connectionMultiplexer.GetDatabase();
        }

        /// <summary>
        /// Gets the connection multiplexer
        /// </summary>
        /// <returns>Connection multiplexer</returns>
        public ConnectionMultiplexer GetConnectionMultiplexer()
        {
            // This is a round robin way of getting the connection on which a request will be executed
            // Locking the cacheStoreCount for parallel connections
            long cacheStoreIndex = Interlocked.Increment(ref this.cacheStoreCount) % this.lazyConnections.Length;
            var connectionMultiplexer = this.lazyConnections[cacheStoreIndex].Value;
            return connectionMultiplexer;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <param name="disposing">Boolean to indicate dispose</param>
        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                var len = this.lazyConnections.Length;
                for (int i = 0; i < len; i++)
                {
                    this.lazyConnections[i].Value.Dispose();
                }
            }
        }

        /// <summary>
        /// Gets the configuration options
        /// </summary>
        /// <param name="connectionString">Redis connection string</param>
        /// <param name="identifier">Identifier for the config for the corresponding connection multiplexer</param>
        /// <returns>Configuration option</returns>
        private ConfigurationOptions GetConfigurationOptions(string connectionString, int identifier)
        {
            // Parses the connection string and sets the other parameters for the Azure Redis Connection
            var config = ConfigurationOptions.Parse(connectionString);
            config.AllowAdmin = true;
            config.AbortOnConnectFail = false;
            config.ClientName = string.Format(CultureInfo.InvariantCulture, "{0}{1}", ClientIdPrefix, identifier);
            return config;
        }
    }
}