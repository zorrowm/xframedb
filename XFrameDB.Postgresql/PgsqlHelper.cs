using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using XFrame.Database.Log;

namespace XFrameDB.Postgresql
{
    /// <summary>
    /// Postgres数据库ADO.NET访问类
    /// "PgIndex"连接字符串: "Host=172.16.108.108;Database=imgindexdb;Username=postgres;Password=postgres;port=5435;",
    /// </summary>
    public class PgsqlHelper:IDisposable
    {
        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        private static string pgConnStr;
        /// <summary>
        /// 数据库连接对象
        /// </summary>
        private NpgsqlConnection dbConnection;

        public PgsqlHelper(string pconnStr)
        {
            SetConnectionString(pconnStr);
        }

        public void SetConnectionString(string pconnStr)
        {
            if (!string.IsNullOrEmpty(pconnStr))
            {
                pgConnStr = pconnStr;
                if (dbConnection!= null)
                {
                    dbConnection.Close();
                    dbConnection.Dispose();
                }
                dbConnection = new NpgsqlConnection(pgConnStr);
            }
            
        }
        public NpgsqlConnection NewDBPgConnection()
        {
            return new NpgsqlConnection(pgConnStr);
        }
        public NpgsqlConnection DBPgConnection {
            get {
                if (dbConnection == null)
                    dbConnection = new NpgsqlConnection(pgConnStr);
                return dbConnection;
            }
         }

        public void Open()
        { 
            if(dbConnection==null)
                dbConnection = new NpgsqlConnection(pgConnStr);
            try
            {
                dbConnection.Open();
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("连接PG数据库错误！", ex);
            }

        }

        public void Close()
        {
            if (dbConnection != null)
                dbConnection.Close();
        }

        /// <summary>
        /// 批量执行删除、更新等命令
        /// </summary>
        /// <param name="sqlList"></param>
        public bool ExcuteSQLList(List<string> sqlList)
        {
            using (NpgsqlConnection conn = NewDBPgConnection())
            {
                NpgsqlTransaction transaction = null;
                try
                {
                    conn.Open();
                    transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                    sqlList.ForEach(p =>
                    {
                        NpgsqlCommand cmdExcute = new NpgsqlCommand(p, conn, transaction);
                          cmdExcute.ExecuteNonQuery();
                    });
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    SystemLogger.getLogger().Error("连接数据库或执行SQL错误：" , ex);
                    return false;
                }
                finally
                {
                    transaction.Dispose();
                    conn.Close();

                }
                return true;
            }
        }

        /// <summary>
        /// 执行单条事务
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public bool ExecuteTransaction(string sql)
        {
            using (NpgsqlConnection conn = NewDBPgConnection())
            {
                NpgsqlTransaction transaction = null;
                int result = 0;
                try
                {
                    conn.Open();
                    transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                    NpgsqlCommand cmdExcute = new NpgsqlCommand(sql, conn, transaction);
                    result= cmdExcute.ExecuteNonQuery();
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    SystemLogger.getLogger().Error("连接数据库或执行SQL错误：", ex);
                    return false;
                }
                finally
                {
                    transaction.Dispose();
                    conn.Close();

                }
                return result>0;
            }
        }


        /// <summary>
        /// 单条命令执行 新增、删除等
        /// </summary>
        /// <param name="executeSQL"></param>
        /// <returns></returns>
        public int ExecuteNonQuery(string executeSQL)
        {
            int result = -1;
            try
            {
                Open();
                NpgsqlCommand cmdExcute = new NpgsqlCommand(executeSQL, this.dbConnection);
                result= cmdExcute.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("数据库ExecuteNonQuery单条命令执行 新增、删除等！", ex);
            }
            finally
            {
                Close();
            }
            return result;
        }

        /// <summary>
        /// 判断表名是否存在
        /// </summary>
        /// <param name="tabName"></param>
        /// <param name="scheme"></param>
        /// <returns></returns>
        public bool ExistTable(string tabName, string scheme = "public")
        {
            string existIndexTab = $"(select 1 from pg_tables   where schemaname = '{scheme}' and tablename = '{tabName}') ";
            bool exist = ExcuteScalar(existIndexTab)!=null;
            return exist;
        }

        /// <summary>
        /// 查询单条记录
        /// </summary>
        /// <param name="executeSQL"></param>
        /// <returns></returns>
        public object ExcuteScalar(string executeSQL)
        {
            try
            {
               Open();
                NpgsqlCommand cmdExcute = new NpgsqlCommand(executeSQL, this.dbConnection);
                return cmdExcute.ExecuteScalar();
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("数据库ExcuteScalar单条记录查询！", ex);
            }
            finally
            {
                Close();
            }
            return null;
        }
        public void Dispose()
        {
            if (dbConnection != null)
            {
                dbConnection.Close();
                dbConnection.Dispose();
                dbConnection = null;
            }
        }

        /// <summary>
        /// 查询单条记录
        /// </summary>
        /// <param name="executeSQL"></param>
        /// <returns></returns>
        public DataRow GetDataRow(string executeSQL)
        {
            try
            {
                Open();
                NpgsqlCommand cmdExcute = new NpgsqlCommand(executeSQL, this.dbConnection);
                NpgsqlDataAdapter dbDataAdapter = new NpgsqlDataAdapter();
                dbDataAdapter.SelectCommand = cmdExcute;
                DataTable dt = new DataTable();
                dbDataAdapter.Fill(dt);
                dbDataAdapter.Dispose();
                if (dt.Rows.Count > 0)
                {
                    DataRow dataRow = dt.Rows[0];
                    return dataRow;
                }

            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("数据库ExcuteScalar单条记录查询！", ex);
            }
            finally
            {
                Close();
            }
            return null;
        }

        /// <summary>
        /// 查询表格的记录总数
        /// </summary>
        /// <param name="tabName"></param>
        /// <returns></returns>
        public int GetCountByTable(string tabName)
        {
            string sql = "select count(*) from " + tabName;
            object value = ExcuteScalar(sql);
            int count = (value == null ? 0 : int.Parse(value.ToString()));
            return count;
        }

        /// <summary>
        /// 查询记录数
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public int GetCount(string sql)
        {
            object value = ExcuteScalar(sql);
            int count = (value == null ? 0 : int.Parse(value.ToString()));
            return count;
        }

        /// <summary>
        /// 查询某一字段的列表
        /// </summary>
        /// <param name="executeSQL"></param>
        /// <returns></returns>
        public List<string> ExecuteSingleFieldList(string strSQL)
        {
            List<string> listObjects = null;
            try
            {       
                Open();
                NpgsqlCommand cmd = new NpgsqlCommand(strSQL, this.dbConnection);
                NpgsqlDataReader reader = cmd.ExecuteReader();
                if (reader != null && reader.HasRows)
                {
                    listObjects = new List<string>();
                    while (reader.Read())
                    {
                        string tmp = reader.GetValue(0)?.ToString();
                        listObjects.Add(tmp);
                    }
                    return listObjects;
                }
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("执行SQL错误" + strSQL, ex);
            }
            finally
            {
                Close();
            }
            return listObjects;
        }


        #region 外部维护数据库连接对象

        /// <summary>
        /// 返回访问数据库的DataReader对象
        /// </summary>
        /// <param name="strSQL"></param>
        /// <param name=""></param>
        /// <param name="pdbConnection"></param>
        /// <returns></returns>
        public NpgsqlDataReader ExecuteDataReader(string strSQL, NpgsqlConnection pdbConnection)
        {
            NpgsqlConnection dbConn = pdbConnection;
            NpgsqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            NpgsqlDataReader reader = cmd.ExecuteReader();
            return reader;
        }

        /// <summary>
        ///  单条命令执行 新增、删除等
        /// </summary>
        /// <param name="executeSQL"></param>
        /// <param name="pdbConnection">外部维护数据库连接对象</param>
        /// <returns></returns>
        public int ExecuteNonQuery(string executeSQL, NpgsqlConnection pdbConnection)
        {
            int result = -1;
            try
            {
                NpgsqlCommand cmdExcute = new NpgsqlCommand(executeSQL, pdbConnection);
                result = cmdExcute.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("数据库ExecuteNonQuery单条命令执行 新增、删除等！", ex);
            }
            return result;
        }

        #endregion

        #region  异步访问方法
        /// <summary>
        /// 异步查询某一字段的列表
        /// </summary>
        /// <param name="executeSQL"></param>
        /// <returns></returns>
        public async Task<List<string>> ExecuteSingleFieldListAsync(string strSQL)
        {
            List<string> listObjects = null;
            try
            {
                Open();
                NpgsqlCommand cmd = new NpgsqlCommand(strSQL, this.dbConnection);
                using (NpgsqlDataReader reader =await cmd.ExecuteReaderAsync())
                {
                    if (reader != null && reader.HasRows)
                    {
                        listObjects = new List<string>();
                        while (await reader.ReadAsync())
                        {
                            string tmp = reader.GetValue(0)?.ToString();
                            listObjects.Add(tmp);
                        }
                        return listObjects;
                    }
                }

            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("执行SQL错误" + strSQL, ex);
            }
            finally
            {
                Close();
            }
            return listObjects;
        }

        /// <summary>
        /// 异步执行
        /// 异步执行--查询单个结果
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="pdbConnection"></param>
        /// <returns></returns>
        public async Task<object>  ExecuteScalarAsync(string sql, NpgsqlConnection pdbConnection)
        {
            using (var command = pdbConnection.CreateCommand())
            {
                command.CommandText = sql;
                command.CommandType = CommandType.Text;
               return await command.ExecuteScalarAsync();
            }
        }
        /// <summary>
        /// 单次连接，单次执行
        /// 异步执行--查询单个结果
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public async Task<object> ExecuteScalarAsync(string sql)
        {
            try
            {
                Open();
                using (var command = this.dbConnection.CreateCommand())
                {
                    command.CommandText = sql;
                    command.CommandType = CommandType.Text;
                  return  await command.ExecuteScalarAsync();
                }
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("执行SQL错误" + sql, ex);
            }
            finally
            {
                this.Close();
            }
            return null;
        }

        /// <summary>
        /// 异步执行
        /// 更新、删除等命令
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="pdbConnection"></param>
        /// <returns></returns>
        public async Task<int> ExecuteNonQueryAsync(string sql,NpgsqlConnection pdbConnection)
        {
            using (var command = pdbConnection.CreateCommand())
            {
                command.CommandText = sql;
                command.CommandType = CommandType.Text;
                return await command.ExecuteNonQueryAsync();
            }
        }
        /// <summary>
        /// 单次连接，单次执行
        /// 异步执行-- 更新、删除等命令
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public async Task<int> ExecuteNonQueryAsync(string sql)
        {
            try {
                Open();
                using (var command = this.dbConnection.CreateCommand())
                {
                    command.CommandText = sql;
                    command.CommandType = CommandType.Text;
                  return await command.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("执行SQL错误" + sql, ex);
            }
            finally
            {
                this.Close();
            }
            return 0;

        }
        /// <summary>
        /// 查询数据表记录
        /// </summary>
        /// <param name="strSQL"></param>
        /// <returns></returns>
        public async Task<DataTable> GetTableResult(string strSQL)
        {
            try
            {
                Open();
                using (var command = new NpgsqlCommand(strSQL, this.dbConnection))
                {
                    command.CommandType = CommandType.Text;
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (reader!=null&&reader.HasRows)
                        {
                            var table = new DataTable();
                            table.Load(reader);

                            return table;
                        }
                        else
                            return null;
                    }
                }
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("执行SQL错误" + strSQL, ex);
            }
            finally
            {
                this.Close();
            }
            return null;
        }
        #endregion


        #region 以前框架的，暂不需要
        ///// <summary>
        ///// 更新
        ///// </summary>
        ///// <param name="tabName"></param>
        ///// <param name="fieldValueDiction"></param>
        ///// <param name="useField"></param>
        ///// <returns></returns>
        //public bool updateByParams(string tabName, IDictionary<string, object> fieldValueDiction, string useField = "")
        //{
        //    bool blexecuted = false;
        //    if (fieldValueDiction.Count > 0)
        //    {
        //        //数据库查询
        //        NpgsqlConnection dbConn = this.open() as NpgsqlConnection;
        //        NpgsqlCommand cmd = dbConn.CreateCommand();
        //        Dictionary<string, NpgsqlDbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
        //        StringBuilder sb = new StringBuilder();
        //        sb.Append("update ");
        //        sb.Append(tabName);
        //        sb.Append(" set ");
        //        int i = 0;
        //        string keyFieldName = "";
        //        object keyFieldValue = null;
        //        foreach (string fieldKey in fieldValueDiction.Keys)
        //        {
        //            if (i == 0)//第一个为条件字段
        //            {
        //                keyFieldName = fieldKey;
        //                keyFieldValue = fieldValueDiction[fieldKey.ToUpper()];
        //                i++;
        //                continue;
        //            }
        //            //if (i > 1)
        //            //{
        //            //    sb.Append(",");
        //            //}
        //            //string tmpField = "field" + (i++);
        //            //sb.Append(fieldKey);
        //            //sb.Append("=:");
        //            //sb.Append(tmpField);

        //            if (i > 0)
        //            {
        //                //sb.Append(", ");
        //                if (i > 1)
        //                {
        //                    sb.Append(",");
        //                }
        //                string tmpField = fieldValueDiction[fieldKey.ToUpper()].ToString();
        //                sb.Append(fieldKey);
        //                sb.Append("='");
        //                sb.Append(tmpField);
        //                sb.Append("'");

        //                //sb.Append("=");
        //                //sb.Append("");
        //                NpgsqlDbType tmpDBType = fieldTypeDiction[fieldKey];
        //                object tmpValue = fieldValueDiction[fieldKey];

        //                //为空 则不更新此字段；即认为空字段是不更新字段
        //                //if (tmpValue == null)
        //                //    continue;

        //                if (tmpDBType == NpgsqlDbType.Date)
        //                {
        //                    DateTime dtime = DateTime.ParseExact(tmpValue.ToString(), "yyyyMMdd", CultureInfo.CurrentCulture);// DateTime.Parse(tmpValue.ToString(),);
        //                    ////构造参数
        //                    cmd.Parameters.Add(tmpField, dtime);
        //                    //cmd.Parameters.Add(tmpField, tmpDBType);
        //                }
        //                else  //构造参数
        //                    cmd.Parameters.Add(tmpField, tmpDBType);
        //            }

        //            i++;
        //        }
        //        sb.Append(" where ");
        //        sb.Append(keyFieldName);
        //        sb.Append(" = ");
        //        sb.Append("'");
        //        sb.Append(keyFieldValue);
        //        sb.Append("'");
        //        string sql = sb.ToString();
        //        sb.Clear();
        //        //SQL语句
        //        cmd.CommandText = sql;
        //        //执行SQL语句
        //        blexecuted = ExecuteTransaction(cmd);

        //        dbConn.Close();
        //        dbConn.Dispose();
        //        //清空释放内存对象
        //        fieldValueDiction.Clear();
        //        fieldValueDiction = null;
        //    }
        //    return blexecuted;

        //    //throw new NotImplementedException();
        //}

        ///// <summary>
        ///// 插入数据
        ///// </summary>
        ///// <param name="tabName"></param>
        ///// <param name="fieldValueDiction"></param>
        ///// <param name="useField"></param>
        ///// <returns></returns>
        //public bool insertByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool useField = true)
        //{
        //    bool blexecuted = false;
        //    if (fieldValueDiction.Count > 0)
        //    {
        //        Open();
        //        //数据库查询
        //        NpgsqlConnection dbConn = this.dbConnection;
        //        NpgsqlCommand cmd = dbConn.CreateCommand();
        //        Dictionary<string, NpgsqlDbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
        //        StringBuilder sb = new StringBuilder();
        //        sb.Append("insert into ");
        //        sb.Append(tabName);

        //        if (useField)
        //        {
        //            sb.Append("(");
        //            bool isFirst = true;
        //            foreach (string field in fieldValueDiction.Keys)
        //            {
        //                if (!isFirst)
        //                {
        //                    sb.Append(",");
        //                }
        //                sb.Append(field);
        //                isFirst = false;
        //            }
        //            sb.Append(")");
        //        }
        //        sb.Append(" values('");
        //        int i = 0;
        //        foreach (string fieldKey in fieldValueDiction.Keys)
        //        {
        //            if (i > 0)
        //            {
        //                sb.Append(",'");
        //            }
        //            //string tmpField = "field" + (i++);
        //            //sb.Append(":" + tmpField);
        //            string tmpField = fieldValueDiction[fieldKey.ToUpper()].ToString();
        //            //sb.Append(fieldKey);
        //            //sb.Append("='");
        //            sb.Append(tmpField);
        //            sb.Append("'");
        //            object tmpValue = fieldValueDiction[fieldKey];
        //            //sb.Append(tmpValue);
        //            if (fieldTypeDiction != null && fieldTypeDiction.Count > 0)
        //            {
        //                NpgsqlDbType tmpDBType = fieldTypeDiction[fieldKey.ToUpper()];
        //                if (tmpDBType == NpgsqlDbType.Date)
        //                {
        //                    DateTime dtime = DateTime.ParseExact(tmpValue.ToString(), "yyyyMMdd", CultureInfo.CurrentCulture);// DateTime.Parse(tmpValue.ToString(),);
        //                    NpgsqlParameter pgParameter = new NpgsqlParameter(tmpField, tmpDBType);
        //                    pgParameter.Value = dtime;
        //                    //构造参数
        //                    cmd.Parameters.Add(pgParameter);
        //                    //cmd.Parameters.Add(tmpField, tmpValue);//(tmpField, tmpDBType, tmpValue, ParameterDirection.Input);
        //                }
        //                else
        //                {
        //                    NpgsqlParameter pgParameter = new NpgsqlParameter(tmpField, tmpDBType);
        //                    pgParameter.Value = tmpValue;
        //                    //构造参数
        //                    cmd.Parameters.Add(pgParameter);
        //                }
        //            }
        //            //else
        //            //    //构造参数
        //            //    cmd.Parameters.Add(tmpField, tmpValue);
        //            i++;
        //        }
        //        sb.Append(" )");
        //        string sql = sb.ToString();
        //        sb.Clear();
        //        //SQL语句
        //        cmd.CommandText = sql;
        //        //清空
        //        fieldTypeDiction.Clear();
        //        fieldTypeDiction = null;
        //        //执行SQL语句
        //        blexecuted = executeTransaction(cmd);

        //        dbConn.Close();
        //        dbConn.Dispose();
        //        //清空释放内存对象
        //        fieldValueDiction.Clear();
        //        fieldValueDiction = null;
        //    }

        //    return blexecuted;
        //}

        /// <summary>
        /// 查询表结构
        /// </summary>
        /// <param name="tabName"></param>
        /// <param name="cmd"></param>
        /// <returns></returns>
        //private Dictionary<string, NpgsqlDbType> getTableFieldType(string tabName, NpgsqlCommand cmd)
        //{
        //    //  string sql = "select * from information_schema.columns where table_schema='public' and table_name=upper('" + tabName + "')";
        //    string sql = "select * from " + tabName + " where 1!=1";
        //    cmd.CommandText = sql;
        //    DataTable dt = new DataTable();
        //    NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
        //    try
        //    {
        //        da.Fill(dt);
        //    }
        //    catch (Exception ex)
        //    {
        //        SystemLogger.getLogger().Error("执行SQL错误：" + sql, ex);
        //    }
        //    finally
        //    {
        //        da.Dispose();
        //    }
        //    int count = dt.Columns.Count;
        //    if (count > 0)
        //    {
        //        Dictionary<string, NpgsqlDbType> result = new Dictionary<string, NpgsqlDbType>(count);
        //        string field;
        //        NpgsqlDbType fieldType = NpgsqlDbType.Varchar;
        //        for (int i = 0; i < count; i++)
        //        {
        //            DataColumn dRow = dt.Columns[i];
        //            field = dRow.ColumnName.ToString().ToUpper();//[0].ToString();
        //            string type = dRow.GetType().ToString();//[1].ToString();
        //            try
        //            {

        //                switch (type)
        //                {
        //                    case "NUMBER":
        //                        if (dRow != null && dRow.ToString() == "0")
        //                            fieldType = NpgsqlDbType.Integer;
        //                        else
        //                            fieldType = NpgsqlDbType.Double;
        //                        break;
        //                    //case "DATE":
        //                    //    fieldType = OracleDbType.TimeStamp;
        //                    //    break;
        //                    default:
        //                        fieldType = (NpgsqlDbType)Enum.Parse(typeof(NpgsqlDbType), type, true);
        //                        break;
        //                }

        //            }
        //            catch (ArgumentException ex)
        //            {
        //                SystemLogger.getLogger().Warn("获取NpgsqlDbType错误!", ex);

        //            }
        //            result.Add(field, fieldType);
        //        }
        //        return result;
        //    }
        //    return null;
        //}
        #endregion


    }
}
