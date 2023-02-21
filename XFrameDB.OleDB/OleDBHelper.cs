using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using XFrame.Database.DBInterface;
using XFrame.Database.Log;

namespace XFrameDB.OleDB
{
    /// <summary>
    /// 用OLEDB连接（打开Excel,Access,SQLServer）
    /// </summary>
    public class OleDBHelper:IDBHelper
    {
        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        private string dbConnString = null;
        public void initConnectString(string connectionString)
        {
            throw new NotImplementedException();
        }

        public System.Data.Common.DbConnection open(string connectionString=null)
        {
             OleDbConnection dbConn  = null;
            if (!string.IsNullOrEmpty(connectionString))
            {
                this.dbConnString = connectionString;
            }
            dbConn = new OleDbConnection(this.dbConnString);
            try
            {
                dbConn.Open();
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("数据库连接失败", ex);
            }
            return dbConn;
        }

        public void close(System.Data.Common.DbConnection dbConn)
        {
            if (dbConn != null && dbConn.State != ConnectionState.Closed)
            {
                dbConn.Close();
            }
        }

        public int execute(string strSQL)
        {
            OleDbConnection dbConn = this.open() as OleDbConnection;
            OleDbCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            OleDbTransaction transaction = dbConn.BeginTransaction();
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

        public int getCountByTable(string tabName)
        {
            string sql = "select count(*) from " + tabName;
            object value = executeScalar(sql);
            int count = (value == null ? 0 : int.Parse(value.ToString()));
            return count;
        }

        public int getCount(string sql)
        {
            object value = executeScalar(sql);
            int count = (value == null ? 0 : int.Parse(value.ToString()));
            return count;
        }

        public System.Data.DataSet executeDataSet(string strSQL)
        {
            OleDbConnection dbConn = this.open() as OleDbConnection;
            OleDbCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            OleDbDataAdapter da = new OleDbDataAdapter(cmd);
            DataSet ds = null;
            try
            {
                ds = new DataSet();
                da.Fill(ds);
                //return ds;
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("执行SQL错误：" + strSQL, ex);
            }
            finally
            {
                da.Dispose();
                dbConn.Close();
                dbConn.Dispose();
            }
            return ds;
        }

        public object executeScalar(string strSQL)
        {
            OleDbConnection dbConn = this.open() as OleDbConnection;
            OleDbCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            try
            {
                return cmd.ExecuteScalar();
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("执行SQL错误：" + strSQL, ex);
            }
            finally
            {
                dbConn.Close();
                dbConn.Dispose();
            }
            return null;
        }

        public List<object[]> executeListObjects(string strSQL)
        {
            OleDbConnection dbConn = this.open() as OleDbConnection;
            OleDbCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            OleDbDataReader reader = null;
            List<object[]> listObjects = null;
            try
            {
                reader = cmd.ExecuteReader();

                if (reader != null && reader.HasRows)
                {
                    int fieldCount = reader.FieldCount;
                    listObjects = new List<object[]>();
                    while (reader.Read())
                    {
                        object[] fieldValues = new object[fieldCount];
                        for (int i = 0; i < fieldCount; i++)
                        {
                            Type fieldType = reader.GetFieldType(i);
                            if (fieldType == Type.GetType("System.DateTime"))
                                fieldValues[i] = reader.GetString(i);//.GetDateTime(i);
                            else
                                fieldValues[i] = reader.GetValue(i);
                        }
                        listObjects.Add(fieldValues);
                    }
                    return listObjects;
                }
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("执行SQL错误" + strSQL, ex);
                return null;
            }
            finally
            {
                dbConn.Close();
                dbConn.Dispose();
            }
            return listObjects;
        }

        public List<string> executeSingleFieldValueList(string strSQL)
        {
            OleDbConnection dbConn = this.open() as OleDbConnection;
            OleDbCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            OleDbDataReader reader = null;
            List<string> listObjects = null;
            try
            {
                reader = cmd.ExecuteReader();

                if (reader != null && reader.HasRows)
                {
                    listObjects = new List<string>();
                    while (reader.Read())
                    {
                        string tmp = reader.GetValue(0).ToString();
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
                if(reader!=null)
                reader.Close();
                dbConn.Close();
                dbConn.Dispose();
            }

            return listObjects;
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
            OleDbConnection dbConn = this.open() as OleDbConnection;
            OleDbTransaction transaction = dbConn.BeginTransaction();
            OleDbCommand cmd = dbConn.CreateCommand();
            cmd.Transaction = transaction;
            bool bl = false;
            try
            {
                foreach (string sql in sqlList)
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }
                transaction.Commit();
                bl = true;
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                SystemLogger.getLogger().Error("执行SQL错误：" + cmd.CommandText, ex);
            }
            finally
            {
                dbConn.Close();
                dbConn.Dispose();
            }
            return bl;
        }

        public bool executeTransaction(string strSQL)
        {
            OleDbConnection dbConn = this.open() as OleDbConnection;
            OleDbCommand cmd = dbConn.CreateCommand();
            if (cmd == null || cmd.Connection == null)
            {
                SystemLogger.getLogger().Error("执行SQL错误:OleDbCommand为空！");
                return false;
            }
            cmd.CommandType = CommandType.Text;
            OleDbTransaction transaction = null;

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

        /// <summary>
        /// 以表格形式返回结果
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public DataTable getDataTableResult(string sql)
        {
            //数据库查询
            OleDbConnection dbConn = this.open() as OleDbConnection;
            OleDbCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = sql;

            OleDbDataAdapter da = new OleDbDataAdapter(cmd);

            DataTable dt = null;
            da.ReturnProviderSpecificTypes = false;
            try
            {
                //DataTable dt = new DataTable();
                //da.Fill(dt);
                DataSet ds = new DataSet();
                da.Fill(ds);
                dt = ds.Tables[0];

            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error("执行SQL错误：" + sql, ex);
            }
            finally
            {
                da.Dispose();
                dbConn.Close();
                dbConn.Dispose();
            }

            return dt;
        }

        public System.Data.Common.DbDataReader executeDataReader(string strSQL)
        {
            OleDbConnection dbConn = this.open() as OleDbConnection;
            OleDbCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            OleDbDataReader reader = cmd.ExecuteReader();
            return reader;
        }

        public bool insertByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, bool useField = true)
        {
            throw new NotImplementedException();
        }

        public bool updateByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string useField = "")
        {
            throw new NotImplementedException();
        }


        public bool executeTransaction(System.Data.Common.DbCommand dbCmd)
        {
            throw new NotImplementedException();
        }

        public long insertByParamsReturnSequence(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string sequenceSQL = null)
        {
            throw new NotImplementedException();
        }

        public string insertByParamsReturnSQL(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, bool useField = true)
        {
            throw new NotImplementedException();
        }

        public string insertByParamsReturnSQL(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false)
        {
            throw new NotImplementedException();
        }

        public string updateByParamsReturnSQL(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string useField = "")
        {
            throw new NotImplementedException();
        }
    }
}
