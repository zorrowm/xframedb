using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Reflection;
using XFrame.Database.Log;

namespace XFrameDB.Postgresql
{
    /// <summary>
    /// DataTable 与 Entity 间的转换工具
    /// 用以快速将大批量数据插入到postgresql中
    /// https://blog.csdn.net/wulex/article/details/53561780
    /// https://www.cnblogs.com/Aldebaran/archive/2012/12/28/2837559.html
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    public class TableEntityHelper<TEntity> where TEntity : new()
    {
        /// <summary>
        /// TEntity的属性信息
        /// Dictionary(string "property_name", Type property_type)
        /// </summary>
        public Dictionary<string, PropertyInfo>  PropInfoDiciton { get; private set; }
        /// <summary>
        /// 数据表全名：schema.tableName or tableName
        /// </summary>
        public string FullTableName { get; set; }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="schema">数据表的schema，一般为public</param>
        /// <param name="tableName">数据表的名称</param>
        public TableEntityHelper(string schema, string tableName)
        {
            var PropInfoArray = GetPropertyFromTEntity();
            if(PropInfoArray!=null)
            {
                PropInfoDiciton = new Dictionary<string, PropertyInfo>(PropInfoArray.Length, StringComparer.OrdinalIgnoreCase);
                foreach (var property in PropInfoArray)
                {
                    PropInfoDiciton.Add(property.Name, property);
                }
            }
            if (!string.IsNullOrWhiteSpace(tableName))
            {
                if (string.IsNullOrWhiteSpace(schema))
                {
                    FullTableName = tableName;
                }
                else
                    FullTableName = string.Format("{0}.{1}", schema, tableName);
            }
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="tableName">数据表的名称</param>
        public TableEntityHelper(string tableName)
            : this(null, tableName)
        { }

        /// <summary>
        /// 获取TEntity的属性信息
        /// </summary>
        /// <returns>TEntity的属性信息的列表</returns>
        private PropertyInfo[] GetPropertyFromTEntity()
        {
            Type t = typeof(TEntity);
            PropertyInfo[] typeArgs = t.GetProperties();
            // t.GetProperty(colName.ToLower(),
            //BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
            return typeArgs;
        }

        /// <summary>
        /// 根据TEntity的属性信息构造对应数据表
        /// </summary>
        /// <returns>只有字段信息的数据表</returns>
        public DataTable GetDataTableSchema()
        {
            DataTable dataTable = new DataTable();

            foreach (PropertyInfo tParam in PropInfoDiciton.Values)
            {
                Type propType = tParam.PropertyType;
                //由于 DataSet 不支持 System.Nullable<> 类型，因此要先做判断
                if ((propType.IsGenericType) && (propType.GetGenericTypeDefinition() == typeof(Nullable<>)))
                    propType = propType.GetGenericArguments()[0];
                dataTable.Columns.Add(tParam.Name, propType);
            }

            return dataTable;
        }

        /// <summary>
        /// 根据TEntity可枚举列表填充给定的数据表
        /// </summary>
        /// <param name="entities">TEntity类型的可枚举列表</param>
        /// <param name="dataTable">数据表</param>
        public void FillDataTable(IEnumerable<TEntity> entities, DataTable dataTable)
        {
            if (entities != null && entities.Count() > 0)
            {
                foreach (TEntity entity in entities)
                {
                    FillDataTable(entity, dataTable);
                }
            }
        }

        /// <summary>
        /// 在DataTable中插入单条数据
        /// </summary>
        /// <param name="entity">具体数据</param>
        /// <param name="dataTable">数据表</param>
        public void FillDataTable(TEntity entity, DataTable dataTable)
        {
            var dataRow = dataTable.NewRow();
            foreach (DataColumn col in dataTable.Columns)
            {
                string colName = col.ColumnName;
                dataRow[colName] = PropInfoDiciton[colName].GetValue(entity);
            }
            dataTable.Rows.Add(dataRow);
        }

        /// <summary>
        /// 通过PostgreSQL连接把dataTable中的数据整块填充到数据库对应的数据表中
        /// 注意，该函数不负责NpgsqlConnection的创建、打开以及关闭
        /// </summary>
        /// <param name="conn">PostgreSQL连接</param>
        /// <param name="dataTable">数据表</param>
        public void BulkInsert(NpgsqlConnection conn, DataTable dataTable)
        {
            if (conn.State != ConnectionState.Open)
            {
                conn.Open();
            }
            var commandFormat = string.Format(CultureInfo.InvariantCulture, "COPY {0}  FROM STDIN BINARY", FullTableName);
            using (var writer = conn.BeginBinaryImport(commandFormat))
            {
                foreach (DataRow item in dataTable.Rows)
                    writer.WriteRow(item.ItemArray);
                writer.Complete();
            }
            conn.Close();
        }

        #region DataTable记录转为具体对象
        public  TEntity ConvertToEntity(DataRow tableRow) 
        {
            // Create a new type of the entity I want
            TEntity returnObject = new TEntity();
            var columnCollection = tableRow.Table.Columns;
            foreach (DataColumn col in columnCollection)
            {
                string colName = col.ColumnName;
                //PropertyInfo pInfo = t.GetProperty(colName.ToLower(),
                //    BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
                PropertyInfo pInfo = PropInfoDiciton[colName];
                // did we find the property ?
                if (pInfo != null)
                {
                    object val = tableRow[colName];
                    // is this a Nullable<> type
                    bool IsNullable = (Nullable.GetUnderlyingType(pInfo.PropertyType) != null);
                    if (IsNullable)
                    {
                        if (val is System.DBNull)
                        {
                            val = null;
                        }
                        else
                        {
                            // Convert the db type into the T we have in our Nullable<T> type
                            val = Convert.ChangeType(val, Nullable.GetUnderlyingType(pInfo.PropertyType));
                        }
                    }
                    else
                    {
                        try
                        {
                            // Convert the db type into the type of the property in our entity
                            val = Convert.ChangeType(val, pInfo.PropertyType);
                        }
                        catch (Exception ex)
                        {
                            SystemLogger.getLogger().Error(ex);
                        }

                    }
                    try
                    {
                        // Set the value of the property with the value from the db
                        pInfo.SetValue(returnObject, val, null);
                    }
                    catch (Exception ex)
                    {
                        SystemLogger.getLogger().Error(ex);
                    }
                }
            }
            // return the entity object with values
            return returnObject;
        }
        public  List<TEntity> ConvertToEntityList(DataTable table) 
        {
            // Create a new type of the entity I want
            int capacity = table.Rows.Count;
            List<TEntity> result = new List<TEntity>(capacity);
            foreach (DataRow tableRow in table.Rows)
            {
                TEntity returnObject= ConvertToEntity(tableRow);
                result.Add(returnObject);
            }
            // return the entity object with values
            return result;
        }
        #endregion
    }
}
