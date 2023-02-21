using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace XFrameDB.MongoDB
{
    /// <summary>
    /// MongoDb操作帮助类
    /// 参考：http://www.zuowenjun.cn/post/2018/01/08/253.html
    /// 参考：https://github.com/girlw/MongoDBHelper/blob/master/MongoDbHelper/MongoDBHelper.cs
    /// </summary>
    public class MongoDBHelper : IDisposable
    {
        private readonly string connectionString = null;
        private readonly string databaseName = null;
        private IMongoDatabase database = null;
        private MongoClient client = null;
        private bool autoCreateDb = false;
        private bool autoCreateCollection = false;

        //单例模式
        private static MongoDBHelper mongoDBHelper;
        //线程同步标识
        private static readonly object locker = new object();

        public static MongoDBHelper GetInstance(string mongoConnStr, bool autoCreateDb = false, bool autoCreateCollection = false)
        {
            if (mongoDBHelper == null)
            {
                lock (locker)
                {
                    //如果类的实例不存在则创建，否则直接返回
                    if (mongoDBHelper == null)
                    {
                        mongoDBHelper = new MongoDBHelper(mongoConnStr, autoCreateDb, autoCreateCollection);
                    }
                }
            }
            return mongoDBHelper;
        }

        public static MongoDBHelper GetInstance(string mongoConnStr, string dbName, bool autoCreateDb = false, bool autoCreateCollection = false)
        {
            if (mongoDBHelper == null)
            {
                lock (locker)
                {
                    //如果类的实例不存在则创建，否则直接返回
                    if (mongoDBHelper == null)
                    {
                        mongoDBHelper = new MongoDBHelper(mongoConnStr, dbName, autoCreateDb, autoCreateCollection);
                    }
                }
            }
            return mongoDBHelper;
        }

        public MongoDBHelper(string mongoConnStr, bool autoCreateDb = false, bool autoCreateCollection = false)
        {
            this.connectionString = mongoConnStr;
            this.databaseName = "";
            this.autoCreateDb = autoCreateDb;
            this.autoCreateCollection = autoCreateCollection;
        }

        public MongoDBHelper(string mongoConnStr, string dbName, bool autoCreateDb = false, bool autoCreateCollection = false)
        {
            this.connectionString = mongoConnStr;
            this.databaseName = dbName;
            this.autoCreateDb = autoCreateDb;
            this.autoCreateCollection = autoCreateCollection;
        }

        #region 私有方法

        private MongoClient CreateMongoClient()
        {
            if (client == null)
                client = new MongoClient(connectionString);
            return client;
        }

        private IMongoDatabase GetMongoDatabase()
        {
            if (database == null)
            {
                client = CreateMongoClient();
                if (!DatabaseExists(client, databaseName) && !autoCreateDb)
                {
                    return null;
                    //throw new KeyNotFoundException("此MongoDB名称不存在：" + databaseName);
                }
                database = client.GetDatabase(databaseName);
            }

            return database;
        }

        private bool DatabaseExists(MongoClient client, string dbName)
        {
            try
            {
                return client.ListDatabaseNames().ToList().Contains(dbName);
                //var dbNames = client.ListDatabases().ToList().Select(db => db.GetValue("name").AsString);
                //return dbNames.Contains(dbName);
            }
            catch //如果连接的账号不能枚举出所有DB会报错，则默认为true
            {
                return true;
            }
        }

        private bool CollectionExists(IMongoDatabase database, string collectionName)
        {
            var options = new ListCollectionsOptions
            {
                Filter = Builders<BsonDocument>.Filter.Eq("name", collectionName)
            };
            return database.ListCollections(options).ToEnumerable().Any();
            //return database.ListCollectionNames().ToList().Contains(collectionName);
        }

        private IMongoCollection<TDoc> GetMongoCollection<TDoc>(string name, MongoCollectionSettings settings = null)
        {
            database = GetMongoDatabase();
            if (database == null)
                return null;
            if (!CollectionExists(database, name) && !autoCreateCollection)
            {
                return null;
                //throw new KeyNotFoundException("此Collection名称不存在：" + name);
            }
            return database.GetCollection<TDoc>(name, settings);
        }

        /// <summary>
        /// 获取更新信息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <returns></returns>
        private UpdateDefinition<T> GetUpdateDefinition<T>(T entity)
        {
            var properties = typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public);

            var updateDefinitionList = GetUpdateDefinitionList<T>(properties, entity);

            var updateDefinitionBuilder = new UpdateDefinitionBuilder<T>().Combine(updateDefinitionList);

            return updateDefinitionBuilder;
        }

        /// <summary>
        /// 获取更新信息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="propertyInfos"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        private List<UpdateDefinition<T>> GetUpdateDefinitionList<T>(PropertyInfo[] propertyInfos, object entity)
        {
            var updateDefinitionList = new List<UpdateDefinition<T>>();

            propertyInfos = propertyInfos.Where(a => a.Name != "_id").ToArray();

            foreach (var propertyInfo in propertyInfos)
            {
                if (propertyInfo.PropertyType.IsArray || typeof(IList).IsAssignableFrom(propertyInfo.PropertyType))
                {
                    var value = propertyInfo.GetValue(entity) as IList;

                    var filedName = propertyInfo.Name;

                    updateDefinitionList.Add(Builders<T>.Update.Set(filedName, value));
                }
                else
                {
                    var value = propertyInfo.GetValue(entity);
                    if (propertyInfo.PropertyType == typeof(decimal))
                        value = value.ToString();

                    var filedName = propertyInfo.Name;

                    updateDefinitionList.Add(Builders<T>.Update.Set(filedName, value));
                }
            }

            return updateDefinitionList;
        }


        private List<UpdateDefinition<TDoc>> BuildUpdateDefinition<TDoc>(object doc, string parent)
        {
            var updateList = new List<UpdateDefinition<TDoc>>();
            foreach (var property in typeof(TDoc).GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                var key = parent == null ? property.Name : string.Format("{0}.{1}", parent, property.Name);
                //非空的复杂类型
                if ((property.PropertyType.IsClass || property.PropertyType.IsInterface) && property.PropertyType != typeof(string) && property.GetValue(doc) != null)
                {
                    if (typeof(IList).IsAssignableFrom(property.PropertyType))
                    {
                        #region 集合类型
                        int i = 0;
                        var subObj = property.GetValue(doc);
                        foreach (var item in subObj as IList)
                        {
                            if (item.GetType().IsClass || item.GetType().IsInterface)
                            {
                                updateList.AddRange(BuildUpdateDefinition<TDoc>(doc, string.Format("{0}.{1}", key, i)));
                            }
                            else
                            {
                                updateList.Add(Builders<TDoc>.Update.Set(string.Format("{0}.{1}", key, i), item));
                            }
                            i++;
                        }
                        #endregion
                    }
                    else
                    {
                        #region 实体类型
                        //复杂类型，导航属性，类对象和集合对象 
                        var subObj = property.GetValue(doc);
                        foreach (var sub in property.PropertyType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
                        {
                            updateList.Add(Builders<TDoc>.Update.Set(string.Format("{0}.{1}", key, sub.Name), sub.GetValue(subObj)));
                        }
                        #endregion
                    }
                }
                else //简单类型
                {
                    updateList.Add(Builders<TDoc>.Update.Set(key, property.GetValue(doc)));
                }
            }
            return updateList;
        }

        private void CreateIndex<TDoc>(IMongoCollection<TDoc> col, string[] indexFields, CreateIndexOptions options = null)
        {
            if (col == null || indexFields == null)
            {
                return;
            }
            var indexKeys = Builders<TDoc>.IndexKeys;
            IndexKeysDefinition<TDoc> keys = null;
            if (indexFields.Length > 0)
            {
                keys = indexKeys.Descending(indexFields[0]);
            }
            for (var i = 1; i < indexFields.Length; i++)
            {
                var strIndex = indexFields[i];
                keys = keys.Descending(strIndex);
            }

            if (keys != null)
            {
                CreateIndexModel<TDoc> indexModel = new CreateIndexModel<TDoc>(keys, options);
                col.Indexes.CreateOne(indexModel);//CreateOne(keys, options);
            }
        }

        #endregion

        public void CreateCollectionIndex<TDoc>(string collectionName, string[] indexFields, CreateIndexOptions options = null)
        {
            CreateIndex(GetMongoCollection<TDoc>(collectionName), indexFields, options);
        }

        public void CreateCollection<TDoc>(string[] indexFields = null, CreateIndexOptions options = null)
        {
            string collectionName = typeof(TDoc).Name;
            CreateCollection<TDoc>(collectionName, indexFields, options);
        }

        public void CreateCollection<TDoc>(string collectionName, string[] indexFields = null, CreateIndexOptions options = null)
        {
            database = GetMongoDatabase();
            if (database == null)
                return;
            database.CreateCollection(collectionName);
            CreateIndex(GetMongoCollection<TDoc>(collectionName), indexFields, options);
        }

        #region 查询

        public bool IsExistDataBase(string dbName)
        {
            client = CreateMongoClient();
            return DatabaseExists(client, databaseName);
        }

        public bool IsExistCollection(string collectionName)
        {
            database = GetMongoDatabase();
            if (database == null)
                return false;
            return CollectionExists(database, collectionName);
        }

        public bool IsExistDocument<TDoc>(string collectionName, Expression<Func<TDoc, bool>> filter)
        {
            var colleciton = GetMongoCollection<TDoc>(collectionName);
            if (colleciton == null)
                return false;
            return colleciton.Find(filter, null).CountDocuments() > 0;
        }

        public async System.Threading.Tasks.Task<TDoc> FindOneAsync<TDoc>(string collectionName, Expression<Func<TDoc, bool>> filter, FindOptions options = null)
        {
            var colleciton = GetMongoCollection<TDoc>(collectionName);
            if (colleciton == null)
                return default(TDoc);
            return await colleciton.Find(filter, options).FirstOrDefaultAsync();
        }

        public List<TDoc> Find<TDoc>(Expression<Func<TDoc, bool>> filter, FindOptions options = null)
        {
            string collectionName = typeof(TDoc).Name;
            return Find<TDoc>(collectionName, filter, options);
        }

        public List<TDoc> Find<TDoc>(string collectionName, Expression<Func<TDoc, bool>> filter, FindOptions options = null)
        {
            var colleciton = GetMongoCollection<TDoc>(collectionName);
            if (colleciton == null)
                return null;
            return colleciton.Find(filter, options).ToList();
        }


        public List<TDoc> FindByPage<TDoc, TResult>(Expression<Func<TDoc, bool>> filter, Expression<Func<TDoc, TResult>> keySelector, int pageIndex, int pageSize, out int rsCount)
        {
            string collectionName = typeof(TDoc).Name;
            return FindByPage<TDoc, TResult>(collectionName, filter, keySelector, pageIndex, pageSize, out rsCount);
        }

        public List<TDoc> FindByPage<TDoc, TResult>(string collectionName, Expression<Func<TDoc, bool>> filter, Expression<Func<TDoc, TResult>> keySelector, int pageIndex, int pageSize, out int rsCount)
        {
            var colleciton = GetMongoCollection<TDoc>(collectionName);
            if (colleciton == null)
            {
                rsCount = 0;
                return null;
            }
            rsCount = colleciton.AsQueryable().Where(filter).Count();

            int pageCount = rsCount / pageSize + ((rsCount % pageSize) > 0 ? 1 : 0);
            if (pageIndex > pageCount) pageIndex = pageCount;
            if (pageIndex <= 0) pageIndex = 1;

            return colleciton.AsQueryable(new AggregateOptions { AllowDiskUse = true }).Where(filter).OrderByDescending(keySelector).Skip((pageIndex - 1) * pageSize).Take(pageSize).ToList();
        }
        #endregion

        #region 新增
        public void Insert<TDoc>(TDoc doc, InsertOneOptions options = null)
        {
            string collectionName = typeof(TDoc).Name;
            Insert<TDoc>(collectionName, doc, options);
        }

        public void Insert<TDoc>(string collectionName, TDoc doc, InsertOneOptions options = null)
        {
            var colleciton = GetMongoCollection<TDoc>(collectionName);
            if (colleciton == null)
                return;
            colleciton.InsertOne(doc, options);
        }


        public void InsertMany<TDoc>(IEnumerable<TDoc> docs, InsertManyOptions options = null)
        {
            string collectionName = typeof(TDoc).Name;
            InsertMany<TDoc>(collectionName, docs, options);
        }

        public void InsertMany<TDoc>(string collectionName, IEnumerable<TDoc> docs, InsertManyOptions options = null)
        {
            var colleciton = GetMongoCollection<TDoc>(collectionName);
            if (colleciton == null)
                return;
            colleciton.InsertMany(docs, options);
        }
        #endregion

        #region 更新
        public void Update<TDoc>(TDoc doc, Expression<Func<TDoc, bool>> filter, UpdateOptions options = null)
        {
            string collectionName = typeof(TDoc).Name;
            var colleciton = GetMongoCollection<TDoc>(collectionName);
            if (colleciton == null)
                return;
            List<UpdateDefinition<TDoc>> updateList = BuildUpdateDefinition<TDoc>(doc, null);
            colleciton.UpdateOne(filter, Builders<TDoc>.Update.Combine(updateList), options);
        }

        public void Update<TDoc>(string collectionName, TDoc doc, Expression<Func<TDoc, bool>> filter, UpdateOptions options = null)
        {
            var colleciton = GetMongoCollection<TDoc>(collectionName);
            if (colleciton == null)
                return;
            //List<UpdateDefinition<TDoc>> updateList = BuildUpdateDefinition<TDoc>(doc, null);
            UpdateDefinition<TDoc> updateDef = GetUpdateDefinition<TDoc>(doc);
            colleciton.UpdateOne(filter, updateDef, options);
        }


        public void Update<TDoc>(TDoc doc, Expression<Func<TDoc, bool>> filter, UpdateDefinition<TDoc> updateFields, UpdateOptions options = null)
        {
            string collectionName = typeof(TDoc).Name;
            Update<TDoc>(collectionName, doc, filter, updateFields, options);
        }

        public void Update<TDoc>(string collectionName, TDoc doc, Expression<Func<TDoc, bool>> filter, UpdateDefinition<TDoc> updateFields, UpdateOptions options = null)
        {
            var colleciton = GetMongoCollection<TDoc>(collectionName);
            if (colleciton == null)
                return;
            colleciton.UpdateOne(filter, updateFields, options);
        }


        public void UpdateMany<TDoc>(TDoc doc, Expression<Func<TDoc, bool>> filter, UpdateOptions options = null)
        {
            string collectionName = typeof(TDoc).Name;
            UpdateMany<TDoc>(collectionName, doc, filter, options);
        }


        public void UpdateMany<TDoc>(string collectionName, TDoc doc, Expression<Func<TDoc, bool>> filter, UpdateOptions options = null)
        {
            var colleciton = GetMongoCollection<TDoc>(collectionName);
            if (colleciton == null)
                return;
            List<UpdateDefinition<TDoc>> updateList = BuildUpdateDefinition<TDoc>(doc, null);
            colleciton.UpdateMany(filter, Builders<TDoc>.Update.Combine(updateList), options);
        }
        #endregion

        #region 删除
        public void Delete<TDoc>(Expression<Func<TDoc, bool>> filter, DeleteOptions options = null)
        {
            string collectionName = typeof(TDoc).Name;
            Delete<TDoc>(collectionName, filter, options);
        }

        public void Delete<TDoc>(string collectionName, Expression<Func<TDoc, bool>> filter, DeleteOptions options = null)
        {
            var colleciton = GetMongoCollection<TDoc>(collectionName);
            if (colleciton == null)
                return;
            colleciton.DeleteOne(filter, options);
        }


        public void DeleteMany<TDoc>(Expression<Func<TDoc, bool>> filter, DeleteOptions options = null)
        {
            string collectionName = typeof(TDoc).Name;
            DeleteMany<TDoc>(collectionName, filter, options);
        }


        public void DeleteMany<TDoc>(string collectionName, Expression<Func<TDoc, bool>> filter, DeleteOptions options = null)
        {
            var colleciton = GetMongoCollection<TDoc>(collectionName);
            if (colleciton == null)
                return;
            colleciton.DeleteMany(filter, options);
        }
        public async System.Threading.Tasks.Task DropDatabaseAsync(string databaseName)
        {
            client = CreateMongoClient();
            await client.DropDatabaseAsync(databaseName);
        }
        public void ClearCollection<TDoc>(string collectionName)
        {
            var database = GetMongoDatabase();
            if (database == null || !CollectionExists(database, collectionName))
            {
                return;
            }
            var colleciton = GetMongoCollection<TDoc>(collectionName);
            if (colleciton == null)
                return;
            var inddexs = colleciton.Indexes.List();
            List<IEnumerable<BsonDocument>> docIndexs = new List<IEnumerable<BsonDocument>>();
            while (inddexs.MoveNext())
            {
                docIndexs.Add(inddexs.Current);
            }
            database.DropCollection(collectionName);

            //if (!CollectionExists(mongoDatabase, collectionName))
            //{
            //    CreateCollection<TDoc>(collectionName);
            //}

            //if (docIndexs.Count > 0)
            //{
            //    colleciton = mongoDatabase.GetCollection<TDoc>(collectionName);
            //    foreach (var index in docIndexs)
            //    {
            //        foreach (IndexKeysDefinition<TDoc> indexItem in index)
            //        {
            //            try
            //            {
            //                colleciton.Indexes.CreateOne(indexItem);
            //            }
            //            catch
            //            { }
            //        }
            //    }
            //}
        }
        #endregion

        #region 执行命令
        public BsonDocument RunCommand(string cmdText)
        {
            database = GetMongoDatabase();
            if (database == null)
                return null;
            return database.RunCommand<BsonDocument>(cmdText);
        }

        #endregion


        public void Dispose()
        {
            if (database != null)
                database = null;
            if (client != null)
                client = null;
            GC.Collect();
        }
    }
}
