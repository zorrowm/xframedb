using StackExchange.Redis;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace XFrameDB.StackRedis
{
    /// <summary>
    /// Redis帮助工具类
    /// 参考实现：https://github.com/dotnet/extensions/blob/f4066026ca06984b07e90e61a6390ac38152ba93/src/Caching/StackExchangeRedis/src/RedisCache.cs
    ///  "Redis"连接字符串: "172.16.108.108:6379,defaultDatabase=0,connectTimeout=15000" 
    /// </summary>
    public class StackRedisHelper:IDisposable
    {
        private string redisConnStr = null;
        private  IServer redisServer;
        private ConfigurationOptions _options = null;
        private IDatabase database = null;
        private ConnectionMultiplexer _connection;

        private readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(initialCount: 1, maxCount: 1);
        public StackRedisHelper(string connRedis)
        {
            redisConnStr = connRedis;
            _options = ConfigurationOptions.Parse(redisConnStr);
            // Microsoft Azure team wants abortConnect=false by default
            _options.AbortOnConnectFail = false;
            //options.ConnectTimeout = 10000;//默认是5s
            //options.ReconnectRetryPolicy=
        }

        private void Connect()
        {
            if (database != null)
            {
                return;
            }
            _connectionLock.Wait();
            try
            {
                if (database == null)
                {
                    if (_options != null)
                    {
                        _connection = ConnectionMultiplexer.Connect(_options);
                    }
                    database = _connection.GetDatabase();
                }
                if(redisServer==null)
                    redisServer = _connection.GetServer(_options.EndPoints.First());
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        private async Task ConnectAsync()
        {

            if (database != null)
            {
                return;
            }

            await _connectionLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (database == null)
                {
                    if (_options != null)
                    {
                        _connection = await ConnectionMultiplexer.ConnectAsync(_options).ConfigureAwait(false);
                    }
                    else
                    {
                        _connection = await ConnectionMultiplexer.ConnectAsync(_options).ConfigureAwait(false);
                    }

                    database = _connection.GetDatabase();
                }
                if (redisServer == null)
                    redisServer = _connection.GetServer(_options.EndPoints.First());
            }
            finally
            {
                _connectionLock.Release();
            }
        }


        public bool Save(string key, string value, int seconds = 0)
        {
            Connect();
            bool result = false;
            if (seconds == 0)
            {
                result = database.StringSet(key, value);//设置StringSet(key, value)
            }
            else
                result = database.StringSet(key, value, TimeSpan.FromSeconds(seconds));//设置时间，10s后过期。
            return result;
        }
        public async Task<bool> SaveAsync(string key, string value, int seconds = 0)
        {
            await ConnectAsync().ConfigureAwait(false);
            bool result = false;
            if (seconds == 0)
            {
                result = await database.StringSetAsync(key, value).ConfigureAwait(false);//设置StringSet(key, value)
            }
            else
                result = await database.StringSetAsync(key, value, TimeSpan.FromSeconds(seconds)).ConfigureAwait(false);//设置时间，10s后过期。
            return result;
        }

        public string Get(string key)
        {
            Connect();
            return database.StringGet(key);
        }
        public async Task<string> GetAsync(string key)
        {
            await ConnectAsync().ConfigureAwait(false);
            return await database.StringGetAsync(key).ConfigureAwait(false);
        }

        public bool ContainsKey(string key)
        {
            Connect();
            return database.KeyExists(key);
        }

        public async Task<bool> ContainsKeyAsync(string key)
        {
            await ConnectAsync().ConfigureAwait(false);
            return await database.KeyExistsAsync(key).ConfigureAwait(false);
        }

        public bool ClearKeysByPattern(string patternKey)
        {
            Connect();
            var keyArray = redisServer.Keys(pattern: $"{patternKey}").ToArray();
            if (keyArray == null || keyArray.Length == 0)
                return false;
            database.KeyDelete(keyArray);
            return true;
        }
        public async Task<bool> ClearKeysByPatternAsync(string patternKey)
        {
            await ConnectAsync().ConfigureAwait(false);
            var keyArray = redisServer.Keys(pattern: $"{patternKey}").ToArray();
            if (keyArray == null || keyArray.Length == 0)
                return false;
            await database.KeyDeleteAsync(keyArray).ConfigureAwait(false);
            return true;
        }
        public void Dispose()
        {
            if (_connection != null)
            {
                _connection.Close();
            }
        }
}
}
