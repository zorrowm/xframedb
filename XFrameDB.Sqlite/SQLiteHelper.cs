using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.IO;
using System.Text;
using XFrame.Database.DBInterface;
using XFrame.Database.Log;

namespace XFrameDB.SQLite
{
    /// <summary>
    /// SQLite数据库访问类及方法
    /// </summary>
    public class SQLiteHelper : IDBHelper
    {
        /// <summary>
        /// 系统日志
        /// </summary>
        //public static readonly ILog logger = LogManager.GetLogger("SQLiteHelper");
        //数据库路径
        private string dbpath;
        /// <summary>
        /// 成果数据库路径
        /// </summary>
        public string DBpath
        {
            get { return dbpath; }
        }
        /// <summary>
        /// 任务构造函数
        /// </summary>
        public SQLiteHelper(string messageDBPath)
        {
            this.dbpath = messageDBPath;
        }

        public void initConnectString(string dbPath)
        {
            this.dbpath = dbPath;
        }
        /// <summary>
        /// 连接数据库
        /// </summary>
        /// <param name="dbFile">数据库路径</param>
        /// <returns></returns>
        public DbConnection open(string dbFile = null)
        {
            DbConnection sqlConn = null;
            if (dbFile != null && File.Exists(dbFile))
            {
                this.dbpath = dbFile;
            }
            if (sqlConn == null || sqlConn.State == ConnectionState.Closed)
            {
                sqlConn = new SQLiteConnection("Data Source=" + this.dbpath);
                sqlConn.Open();
            }
            else
            {
                sqlConn.Close();
                sqlConn = new SQLiteConnection("Data Source=" + this.dbpath);
                sqlConn.Open();
            }
            return sqlConn;
        }

        public DataTable getDataTableResult(string sql)
        {
            //数据库查询
            SQLiteConnection sqlConn = (SQLiteConnection)open();

            SQLiteCommand cmd = sqlConn.CreateCommand();
            cmd.CommandText = sql;

            SQLiteDataAdapter da = new SQLiteDataAdapter(cmd);
            try
            {
                DataTable dt = new DataTable();
                da.Fill(dt);
                return dt;
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("执行SQL错误：" + sql, ex);
            }
            finally
            {
                da.Dispose();
                sqlConn.Close();
            }
            return null;
        }
        /// <summary>
        /// 执行数据库命令
        /// </summary>
        /// <param name="strSQL">SQL命令行</param>
        /// <returns>影响的行数</returns>
        public int executeSql(string strSQL)
        {
            //数据库查询
            SQLiteConnection sqlConn = (SQLiteConnection)open();
            SQLiteCommand cmd = new SQLiteCommand();
            cmd.Connection = sqlConn;
            cmd.CommandText = strSQL;
            int result = 0;
            try
            {
                result = cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("SQL：" + strSQL, ex);
            }
            return result;
        }

        /// <summary>
        /// 返回数据集合
        /// </summary>
        /// <param name="cmdText">SQL语句</param>
        /// <returns></returns>
        public DataSet executeDataSet(string cmdText)
        {
            DataSet ds = new DataSet();
            SQLiteConnection sqlConn = (SQLiteConnection)open();
            SQLiteCommand command = new SQLiteCommand(sqlConn);
            command.CommandText = cmdText;
            SQLiteDataAdapter da = new SQLiteDataAdapter(command);
            da.Fill(ds);
            SystemLogger.getLogger().Debug("SQL：" + cmdText);

            return ds;
        }
        /// <summary>
        /// 获取记录数目
        /// </summary>
        /// <param name="sql">sql语句</param>
        /// <returns></returns>
        public int getCount(string sql)
        {
            object value = executeScalar(sql);
            int count = (value == null ? 0 : int.Parse(value.ToString()));
            return count;
        }

        /// <summary>
        /// 获取记录数目
        /// </summary>
        /// <param name="tabName">表名</param>
        /// <returns></returns>
        public int getCountByTable(string tabName)
        {
            string sql = "select count(*) from " + tabName;
            object value = executeScalar(sql);
            int count = (value == null ? 0 : int.Parse(value.ToString()));
            return count;
        }

        /// <summary>
        /// 执行SQL语句（更新、删除）
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public bool execute2(string sql)
        {
            int result = executeSql(sql);
            return result > 0 ? true : false;
        }

        /// <summary>
        /// 返回结果集中的第一行第一列，忽略其他行或列
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="commandParameters">传入的参数</param>
        /// <returns></returns>
        public object executeScalar(string cmdText)
        {
            SQLiteConnection sqlConn = (SQLiteConnection)open();
            SQLiteCommand command = new SQLiteCommand(sqlConn);
            command.CommandText = cmdText;
            SystemLogger.getLogger().Debug("SQL：" + cmdText);
            return command.ExecuteScalar();
        }

        /// <summary>
        /// 返回表记录数据
        /// </summary>
        /// <param name="cmdText"></param>
        /// <returns></returns>
        public List<object[]> executeListObjects(string cmdText)
        {
            SQLiteConnection sqlConn = (SQLiteConnection)open();
            List<object[]> listObjects = null;
            SQLiteCommand command = new SQLiteCommand(sqlConn);
            command.CommandText = cmdText;
            try
            {
                SQLiteDataReader reader = command.ExecuteReader();
                if (reader.HasRows)
                {
                    int fieldCount = reader.FieldCount;
                    listObjects = new List<object[]>();
                    while (reader.Read())
                    {
                        object[] fieldValues = new object[fieldCount];
                        for (int i = 0; i < fieldCount; i++)
                        {
                            if (!reader.IsDBNull(i))
                            {
                                Type fieldType = reader.GetFieldType(i);
                                if (fieldType == Type.GetType("System.DateTime"))
                                    fieldValues[i] = reader.GetString(i);//.GetDateTime(i);
                                else
                                    fieldValues[i] = reader.GetValue(i);
                            }
                            else
                                fieldValues[i] = null;
                        }
                        listObjects.Add(fieldValues);
                    }
                }
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("SQL：" + cmdText, ex);
            }
            return listObjects;
        }

        /// <summary>
        /// 返回单个字段的表记录数据
        /// </summary>
        /// <param name="cmdText"></param>
        /// <returns></returns>
        public List<string> executeSingleFieldValueList(string cmdText)
        {
            SQLiteConnection sqlConn = (SQLiteConnection)open();
            List<string> listObjects = null;
            SQLiteCommand command = new SQLiteCommand(sqlConn);
            command.CommandText = cmdText;
            SQLiteDataReader reader = command.ExecuteReader();
            SystemLogger.getLogger().Debug("SQL：" + cmdText);
            if (reader.HasRows)
            {
                int fieldCount = reader.FieldCount;
                listObjects = new List<string>();
                while (reader.Read())
                {
                    string tmp = reader.GetValue(0).ToString();
                    listObjects.Add(tmp);
                }
            }
            return listObjects;
        }

        /// <summary>
        /// 从两个数据库中导数据
        /// </summary>
        /// <param name="desDBPath">目标数据库</param>
        /// <param name="tabList">表名列表</param>
        /// <returns></returns>
        public void CovertData(string desDBPath, List<string> tabList)
        {
            using (SQLiteConnection desSqlConn = new SQLiteConnection("Data Source=" + desDBPath))
            {
                desSqlConn.Open();
                //连接
                SQLiteCommand cmd = new SQLiteCommand("attach database '" + this.dbpath + "' as newdb", desSqlConn);
                SystemLogger.getLogger().Debug("SQL：" + cmd.CommandText);
                cmd.ExecuteNonQuery();
                foreach (string tabName in tabList)
                {
                    string sqlCmd = "insert into " + tabName + " select * from newdb." + tabName;
                    cmd = new SQLiteCommand(sqlCmd, desSqlConn);
                    try
                    {
                        cmd.ExecuteNonQuery();
                    }
                    catch (SQLiteException ex)
                    {
                        SystemLogger.getLogger().Warn("SQL：" + sqlCmd, ex);
                    }
                }
            }
        }

        /// <summary>
        ///以事务方式 批量执行SQL语句
        /// </summary>
        /// <param name="sqlList"></param>
        /// <returns></returns>
        public bool executeTransactionSQLList(List<string> sqlList)
        {
            if (sqlList == null || sqlList.Count == 0)
                return false;
            SQLiteConnection sqlConn = (SQLiteConnection)open();
            SQLiteTransaction transaction = sqlConn.BeginTransaction();
            SQLiteCommand cmd = sqlConn.CreateCommand();
            cmd.Transaction = transaction;
            try
            {
                foreach (string sql in sqlList)
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }
                transaction.Commit();
                return true;
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                SystemLogger.getLogger().Error("执行SQL错误：" + cmd.CommandText, ex);
            }
            finally
            {
                sqlConn.Close();
            }
            return false;
        }





        public bool executeTransaction(string strSQL)
        {
            SQLiteConnection sqlConn = (SQLiteConnection)open();
            SQLiteTransaction transaction = sqlConn.BeginTransaction();
            SQLiteCommand cmd = sqlConn.CreateCommand();
            cmd.Transaction = transaction;
            try
            {
                cmd.CommandText = strSQL;
                cmd.ExecuteNonQuery();
                transaction.Commit();
                return true;
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                SystemLogger.getLogger().Error("执行SQL错误：" + cmd.CommandText, ex);
                return false;
            }
            finally
            {
                sqlConn.Close();
            }

        }

        public bool executeTransaction(DbCommand dbCmd)
        {
            SQLiteCommand cmd = (SQLiteCommand)dbCmd;
            if (cmd == null || cmd.Connection == null)
            {
                SystemLogger.getLogger().Error("执行SQL错误:SQLiteCommand为空！");
                return false;
            }
            cmd.CommandType = CommandType.Text;
            SQLiteTransaction transaction = null;

            try
            {
                transaction = cmd.Connection.BeginTransaction();
                cmd.Transaction = transaction;
                int result = cmd.ExecuteNonQuery();//.ExecuteNonQuery();
                transaction.Commit();

                transaction.Dispose();
                transaction = null;
                return result > 0 ? true : false;
            }
            catch (Exception ex)
            {
                if (transaction != null)
                    transaction.Rollback();
                SystemLogger.getLogger().Error("执行SQL错误", ex);
            }
            finally
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                    transaction.Dispose();
                }
            }
            return false;
        }

        public System.Data.Common.DbDataReader executeDataReader(string strSQL)
        {
            SQLiteConnection dbConn = this.open() as SQLiteConnection;
            SQLiteCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            SQLiteDataReader reader = cmd.ExecuteReader();
            return reader;
        }

        public bool insertByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool useField = true)
        {
            bool blexecuted = false;
            if (fieldValueDiction.Count > 0)
            {
                //数据库查询
                SQLiteConnection dbConn = this.open() as SQLiteConnection;
                SQLiteCommand cmd = dbConn.CreateCommand();
                Dictionary<string, DbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
                StringBuilder sb = new StringBuilder();
                sb.Append("insert into ");
                sb.Append(tabName);

                if (useField)
                {
                    sb.Append("(");
                    bool isFirst = true;
                    foreach (string field in fieldValueDiction.Keys)
                    {
                        if (!isFirst)
                        {
                            sb.Append(",");
                        }
                        sb.Append(field);
                        isFirst = false;
                    }
                    sb.Append(")");
                }
                sb.Append(" values(");
                int i = 0;
                foreach (string fieldKey in fieldValueDiction.Keys)
                {
                    if (i > 0)
                    {
                        sb.Append(",");
                    }
                    string tmpField = "field" + (i++);
                    sb.Append(":" + tmpField);
                    object tmpValue = fieldValueDiction[fieldKey];
                    //sb.Append(tmpValue);
                    if (fieldTypeDiction != null && fieldTypeDiction.Count > 0)
                    {
                        DbType tmpDBType = fieldTypeDiction[fieldKey.ToUpper()];
                        SQLiteParameter sqlParameter = new SQLiteParameter(tmpField, tmpValue);
                        sqlParameter.DbType = tmpDBType;
                        sqlParameter.Direction = ParameterDirection.Input;
                        cmd.Parameters.Add(sqlParameter);
                        //    if (tmpDBType == DbType.Date)
                        //    {
                        //        //DateTime dtime = DateTime.ParseExact(tmpValue.ToString(), "yyyyMMdd", CultureInfo.CurrentCulture);// DateTime.Parse(tmpValue.ToString(),);
                        //        ////构造参数
                        //        //cmd.Parameters.Add(tmpField, tmpDBType, dtime, ParameterDirection.Input);
                        //        cmd.Parameters.Add(tmpField, tmpDBType, tmpValue, ParameterDirection.Input);
                        //    }
                        //    else  //构造参数
                        //        cmd.Parameters.Add(tmpField, tmpDBType, tmpValue, ParameterDirection.Input);
                    }
                    else
                    {
                        //构造参数
                        // cmd.Parameters.Add(tmpField, tmpValue);
                        SQLiteParameter sqlParameter = new SQLiteParameter(tmpField, tmpValue);
                        cmd.Parameters.Add(sqlParameter);
                    }
                }
                sb.Append(" )");
                string sql = sb.ToString();
                sb.Clear();
                //SQL语句
                cmd.CommandText = sql;
                //清空
                fieldTypeDiction.Clear();
                fieldTypeDiction = null;
                //执行SQL语句
                blexecuted = executeTransaction(cmd);

                dbConn.Close();
                dbConn.Dispose();

            }

            return blexecuted;
        }

        public bool updateByParams(string tabName, Dictionary<string, object> fieldValueDiction, bool useField = true)
        {
            bool blexecuted = false;
            if (fieldValueDiction.Count > 0)
            {
                //数据库查询
                SQLiteConnection dbConn = this.open() as SQLiteConnection;
                SQLiteCommand cmd = dbConn.CreateCommand();
                Dictionary<string, DbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
                StringBuilder sb = new StringBuilder();
                sb.Append("update ");
                sb.Append(tabName);
                sb.Append(" set ");
                int i = 0;
                string keyFieldName = "";
                object keyFieldValue = null;
                foreach (string fieldKey in fieldValueDiction.Keys)
                {
                    if (i == 0)//第一个为条件字段
                    {
                        keyFieldName = fieldKey;
                        keyFieldValue = fieldValueDiction[fieldKey];
                        i++;
                        continue;
                    }
                    if (i > 1)
                    {
                        sb.Append(",");
                    }
                    string tmpField = "field" + (i++);
                    sb.Append(fieldKey);
                    sb.Append("=:");
                    sb.Append(tmpField);
                    DbType tmpDBType = fieldTypeDiction[fieldKey.ToUpper()];
                    object tmpValue = fieldValueDiction[fieldKey];

                    //为空 则不更新此字段；即认为空字段是不更新字段
                    //if (tmpValue == null)
                    //    continue;
                    SQLiteParameter sqlParameter = new SQLiteParameter(tmpField, tmpValue);
                    sqlParameter.DbType = tmpDBType;
                    sqlParameter.Direction = ParameterDirection.Input;
                    cmd.Parameters.Add(sqlParameter);
                    //if (tmpDBType == DbType.Date)
                    //{
                    //    //DateTime dtime = DateTime.ParseExact(tmpValue.ToString(), "yyyyMMdd", CultureInfo.CurrentCulture);// DateTime.Parse(tmpValue.ToString(),);
                    //    ////构造参数
                    //    //cmd.Parameters.Add(tmpField, tmpDBType, dtime, ParameterDirection.Input);
                    //    cmd.Parameters.Add(tmpField, tmpDBType, tmpValue, ParameterDirection.Input);
                    //}
                    //else  //构造参数
                    //     cmd.Parameters.Add(tmpField, tmpDBType, tmpValue, ParameterDirection.Input);
                }
                sb.Append(" where ");
                sb.Append(keyFieldName);
                sb.Append(" = ");
                sb.Append("'");
                sb.Append(keyFieldValue);
                sb.Append("'");
                string sql = sb.ToString();
                sb.Clear();
                //SQL语句
                cmd.CommandText = sql;
                //执行SQL语句
                blexecuted = executeTransaction(cmd);

                dbConn.Close();
                dbConn.Dispose();
            }
            return blexecuted;
        }

        /// <summary>
        /// 查询表结构
        /// </summary>
        /// <param name="tabName"></param>
        /// <param name="cmd"></param>
        /// <returns></returns>
        private Dictionary<string, DbType> getTableFieldType(string tabName, SQLiteCommand cmd)
        {
            string sql = "select column_name, data_type,data_precision from user_tab_columns where Table_Name=upper('" + tabName + "')";
            cmd.CommandText = sql;
            DataTable dt = new DataTable();
            SQLiteDataAdapter da = new SQLiteDataAdapter(cmd);
            try
            {
                da.Fill(dt);
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("执行SQL错误：" + sql, ex);
            }
            finally
            {
                da.Dispose();
            }
            int count = dt.Rows.Count;
            if (count > 0)
            {
                Dictionary<string, DbType> result = new Dictionary<string, DbType>(count);
                string field;
                DbType fieldType = DbType.String;
                for (int i = 0; i < count; i++)
                {
                    DataRow dRow = dt.Rows[i];
                    field = dRow[0].ToString();
                    string type = dRow[1].ToString();

                    try
                    {
                        switch (type)
                        {
                            case "NUMBER":
                                if (dRow[2] != null && dRow[2].ToString() == "0")
                                    fieldType = DbType.Int32;
                                else
                                    fieldType = DbType.Double;
                                break;
                            //case "DATE":
                            //    fieldType = DbType.TimeStamp;
                            //    break;
                            default:
                                fieldType = (DbType)Enum.Parse(typeof(DbType), type, true);
                                break;
                        }

                    }
                    catch (ArgumentException ex)
                    {
                        SystemLogger.getLogger().Warn("获取DbType错误!", ex);

                    }
                    result.Add(field, fieldType);
                }
                return result;
            }
            return null;
        }

        public int execute(string strSQL)
        {
            SQLiteConnection dbConn = this.open() as SQLiteConnection;
            SQLiteCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            SQLiteTransaction transaction = dbConn.BeginTransaction();
            int result = -1;
            try
            {
                cmd.Transaction = transaction;
                result = cmd.ExecuteNonQuery();
                transaction.Commit();
                //return result;
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                SystemLogger.getLogger().Error("执行SQL错误：" + strSQL, ex);
            }
            finally
            {
                dbConn.Close();
                dbConn.Dispose();
            }
            return result;
        }


        public bool updateByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string useField = "")
        {
            throw new NotImplementedException();
        }

        public long insertByParamsReturnSequence(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string sequenceSQL = null)
        {
            bool result = insertByParams(tabName, fieldValueDiction, releaseObj);
            if (!result)
                return -1;
            if (string.IsNullOrEmpty(sequenceSQL))
                return 0;
            object seq = executeScalar(sequenceSQL);
            if (seq == null)
                return -1;
            return long.Parse(seq.ToString());
        }

        public string insertByParamsReturnSQL(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false)
        {
            throw new NotImplementedException();
        }

        public string updateByParamsReturnSQL(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string useField = "")
        {
            throw new NotImplementedException();
        }

        public bool insertByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, bool useField = true)
        {
            throw new NotImplementedException();
        }
    }
}