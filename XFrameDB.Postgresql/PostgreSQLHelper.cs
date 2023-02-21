using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Text;
using XFrame.Database.DBInterface;
using XFrame.Database.Log;

namespace FrameworkDB.PostgreSQL
{

    /// <summary>
    /// Postgresql 数据库操作
    /// http://pgfoundry.org/
    /// </summary>
    public class PostgreSQLHelper:IDBHelper
    {
        public PostgreSQLHelper() { }
        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        public PostgreSQLHelper(string connStr)
        {
            initConnectString(connStr);
        }

        private string dbConnString = null;
        /// <summary>
        /// 初始化连接字符串
        /// </summary>
        /// <param name="connectionString"></param>
        

        public void initConnectString(string connectionString)
        {
            this.dbConnString = connectionString;
        }

        public System.Data.Common.DbConnection open(string connectionString = null)
        {
            System.Data.Common.DbConnection dbConn = null;
            if (!string.IsNullOrEmpty(connectionString))
            {
                this.dbConnString = connectionString;
            }
            dbConn = new NpgsqlConnection (this.dbConnString);
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
            NpgsqlConnection  dbConn = this.open() as NpgsqlConnection ;
            NpgsqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            NpgsqlTransaction transaction = dbConn.BeginTransaction();
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
            NpgsqlConnection  dbConn = this.open() as NpgsqlConnection ;
            NpgsqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
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
            NpgsqlConnection  dbConn = this.open() as NpgsqlConnection ;
            NpgsqlCommand cmd = dbConn.CreateCommand();
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
            NpgsqlConnection  dbConn = this.open() as NpgsqlConnection ;
            NpgsqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            NpgsqlDataReader reader = null;
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
            NpgsqlConnection  dbConn = this.open() as NpgsqlConnection ;
            NpgsqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            NpgsqlDataReader reader = null;
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
            NpgsqlConnection  dbConn = this.open() as NpgsqlConnection ;
            NpgsqlTransaction transaction = dbConn.BeginTransaction();
            NpgsqlCommand cmd = dbConn.CreateCommand();
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
            NpgsqlConnection  dbConn = this.open() as NpgsqlConnection ;
            NpgsqlCommand cmd = dbConn.CreateCommand();
            if (cmd == null || cmd.Connection == null)
            {
                SystemLogger.getLogger().Error("执行SQL错误:NpgsqlCommand为空！");
                return false;
            }
            cmd.CommandType = CommandType.Text;
            NpgsqlTransaction transaction = null;

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
            NpgsqlConnection  dbConn = this.open() as NpgsqlConnection ;
            NpgsqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = sql;

            NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);

            DataTable dt = null;
            da.ReturnProviderSpecificTypes = false;
            try
            {
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
           NpgsqlConnection  dbConn = this.open() as NpgsqlConnection ;
            NpgsqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            NpgsqlDataReader  reader = cmd.ExecuteReader();
            return reader;
        }

        //public bool insertByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool useField = true)
        //{
        //    return false;
        //    //throw new NotImplementedException();
        //}

        /// <summary>
        /// 更新
        /// </summary>
        /// <param name="tabName"></param>
        /// <param name="fieldValueDiction"></param>
        /// <param name="useField"></param>
        /// <returns></returns>
        public bool updateByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string useField = "")
        {
            bool blexecuted = false;
            if (fieldValueDiction.Count > 0)
            {
                //数据库查询
                NpgsqlConnection dbConn = this.open() as NpgsqlConnection;
                NpgsqlCommand cmd = dbConn.CreateCommand();
                Dictionary<string, NpgsqlDbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
                StringBuilder sb = new StringBuilder();
                sb.Append("update ");
                sb.Append(tabName);
                sb.Append(" set ");
                int i = 0;
                string keyFieldName = "";
                object keyFieldValue = null;
                if (string.IsNullOrWhiteSpace(useField))
                {
                    //查询表的主键
                    string selectsql = $@"SELECT
                        pg_attribute.attname AS colname
                    FROM
                        pg_constraint
                    INNER JOIN pg_class ON pg_constraint.conrelid = pg_class.oid
                    INNER JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid
                    AND pg_attribute.attnum = pg_constraint.conkey[1]
                    WHERE
                        pg_class.relname = '{tabName}'
                    AND pg_constraint.contype = 'p'";
                    keyFieldName = executeScalar(selectsql) + "";
                }
                else
                {
                    keyFieldName = useField;
                }

                foreach (string fieldKey in fieldValueDiction.Keys)
                {
                    if (!fieldTypeDiction.ContainsKey(fieldKey))
                        continue;
                    if (keyFieldName.ToLower() == fieldKey.ToLower())
                    {
                        keyFieldValue = fieldValueDiction[fieldKey];
                        continue;
                    }
                    object tmpValue = fieldValueDiction[fieldKey];

                    if (i > 0)
                    {
                        sb.Append(",");
                    }
                    string tmpField = ":" + fieldKey;
                    i++;
                    sb.Append(fieldKey);
                    sb.Append("=");
                    sb.Append(tmpField);
                    //NpgsqlDbType tmpDBType = fieldTypeDiction[fieldKey];
                    if (fieldTypeDiction != null && fieldTypeDiction.Count > 0)
                    {
                        NpgsqlDbType tmpDBType = fieldTypeDiction[fieldKey];
                        if (tmpDBType == NpgsqlDbType.Date || tmpDBType == NpgsqlDbType.Time || tmpDBType == NpgsqlDbType.TimeTz || tmpDBType == NpgsqlDbType.Timestamp || tmpDBType == NpgsqlDbType.TimestampTz)
                        {
                            try
                            {
                                DateTimeFormatInfo dtFormat = new DateTimeFormatInfo();
                                dtFormat.ShortDatePattern = "yyyy/MM/dd";
                                DateTime dtValue = Convert.ToDateTime(tmpValue, dtFormat);
                                DateTimeOffset dtoffset = new DateTimeOffset(dtValue);
                                cmd.Parameters.AddWithValue(tmpField, dtValue);
                            }
                            catch
                            {
                            }
                        }
                        else if (tmpDBType == NpgsqlDbType.Integer || tmpDBType == NpgsqlDbType.Bigint)
                        {
                            int intValue = Convert.ToInt32(tmpValue);
                            cmd.Parameters.AddWithValue(tmpField, intValue);
                        }
                        else if (tmpDBType == NpgsqlDbType.Double)
                        {
                            double dbValue = Convert.ToDouble(tmpValue);
                            cmd.Parameters.AddWithValue(tmpField, dbValue);
                        }
                        else if (tmpDBType == NpgsqlDbType.Numeric)
                        {
                            decimal dcValue = Convert.ToDecimal(tmpValue);
                            cmd.Parameters.AddWithValue(tmpField, dcValue);
                        }
                        else if (tmpDBType == NpgsqlDbType.Boolean)
                        {
                            bool bValue = Convert.ToBoolean(tmpValue);
                            cmd.Parameters.AddWithValue(tmpField, bValue);
                        }
                        else if (tmpDBType == NpgsqlDbType.Bytea)
                        {
                            byte bValue = Convert.ToByte(tmpValue);
                            cmd.Parameters.AddWithValue(tmpField, bValue);
                        }
                        else if (tmpDBType == NpgsqlDbType.Char)
                        {
                            char chValue = Convert.ToChar(tmpValue);
                            cmd.Parameters.AddWithValue(tmpField, chValue);
                        }
                        else  //构造参数
                            cmd.Parameters.AddWithValue(tmpField, tmpValue);
                    }
                    else
                        //构造参数
                        cmd.Parameters.AddWithValue(tmpField, tmpValue);

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
                //清空释放内存对象
                if (releaseObj)
                {
                    fieldValueDiction.Clear();
                    fieldValueDiction = null;
                }
            }
            return blexecuted;

            //throw new NotImplementedException();
        }

        /// <summary>
        /// 插入数据
        /// </summary>
        /// <param name="tabName"></param>
        /// <param name="fieldValueDiction"></param>
        /// <param name="releaseObj"></param>
        /// <param name="useField"></param>
        /// <returns></returns>
        public bool insertByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, bool useField = true)
        {
            bool blexecuted = false;
            if (fieldValueDiction.Count > 0)
            {
                //数据库查询
                NpgsqlConnection dbConn = this.open() as NpgsqlConnection;
                NpgsqlCommand cmd = dbConn.CreateCommand();
                Dictionary<string, NpgsqlDbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
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
                    //string tmpField = "field" + (i++);
                    //sb.Append(":" + tmpField);
                    string tmpField = tmpField = ":" + fieldKey;
                    //sb.Append(fieldKey);
                    //sb.Append("='");
                    sb.Append(tmpField);
                    sb.Append("");
                    object tmpValue = fieldValueDiction[fieldKey];
                    //sb.Append(tmpValue);
                    if (fieldTypeDiction != null && fieldTypeDiction.Count > 0)
                    {
                        NpgsqlDbType tmpDBType = fieldTypeDiction[fieldKey];
                        if (tmpDBType == NpgsqlDbType.Date || tmpDBType == NpgsqlDbType.Time || tmpDBType == NpgsqlDbType.TimeTz || tmpDBType == NpgsqlDbType.Timestamp || tmpDBType == NpgsqlDbType.TimestampTz)
                        {
                            try
                            {
                                DateTimeFormatInfo dtFormat = new DateTimeFormatInfo();
                                dtFormat.ShortDatePattern = "yyyy/MM/dd";
                                DateTime dtValue = Convert.ToDateTime(tmpValue, dtFormat);
                                DateTimeOffset dtoffset = new DateTimeOffset(dtValue);
                                cmd.Parameters.AddWithValue(tmpField, dtValue);
                            }
                            catch
                            {
                            }
                        }
                        else if (tmpDBType == NpgsqlDbType.Integer || tmpDBType == NpgsqlDbType.Bigint)
                        {
                            int intValue = Convert.ToInt32(tmpValue);
                            cmd.Parameters.AddWithValue(tmpField, intValue);
                        }
                        else if (tmpDBType == NpgsqlDbType.Double)
                        {
                            double dbValue = Convert.ToDouble(tmpValue);
                            cmd.Parameters.AddWithValue(tmpField, dbValue);
                        }
                        else if (tmpDBType == NpgsqlDbType.Numeric)
                        {
                            decimal dcValue = Convert.ToDecimal(tmpValue);
                            cmd.Parameters.AddWithValue(tmpField, dcValue);
                        }
                        else if (tmpDBType == NpgsqlDbType.Boolean)
                        {
                            bool bValue = Convert.ToBoolean(tmpValue);
                            cmd.Parameters.AddWithValue(tmpField, bValue);
                        }
                        else if (tmpDBType == NpgsqlDbType.Bytea)
                        {
                            byte bValue = Convert.ToByte(tmpValue);
                            cmd.Parameters.AddWithValue(tmpField, bValue);
                        }
                        else if (tmpDBType == NpgsqlDbType.Char)
                        {
                            char chValue = Convert.ToChar(tmpValue);
                            cmd.Parameters.AddWithValue(tmpField, chValue);
                        }
                        else  //构造参数
                            cmd.Parameters.AddWithValue(tmpField, tmpValue);
                    }
                    else
                        //构造参数
                        cmd.Parameters.AddWithValue(tmpField, tmpValue);
                    i++;
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
                //清空释放内存对象
                if (releaseObj)
                {
                    fieldValueDiction.Clear();
                    fieldValueDiction = null;
                }
            }

            return blexecuted;
        }

        public long insertByParamsReturnSequence(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string sequenceSQL = null)
        {
            bool result = insertByParams(tabName, fieldValueDiction,releaseObj);
            if (!result)
                return -1;
            if (string.IsNullOrEmpty(sequenceSQL))
                return 0;
            object seq = executeScalar(sequenceSQL);
            if (seq == null)
                return -1;
            return long.Parse(seq.ToString());
        }
        /// <summary>
        /// 查询表结构
        /// </summary>
        /// <param name="tabName"></param>
        /// <param name="cmd"></param>
        /// <returns></returns>
        private Dictionary<string,NpgsqlDbType> getTableFieldType(string tabName, NpgsqlCommand cmd)
        {
            //string sql = "select * from " + tabName +" where 1!=1";
            //string sql = "select * from information_schema.columns where table_schema='public' and table_name=upper('" + tabName + "')";
            string sql = $"select ordinal_position as colorder,column_name as columnname,data_type as typename from information_schema.columns where table_schema = 'public' and table_name = '{tabName}' order by ordinal_position asc";
            cmd.CommandText = sql;
            DataTable dt = new DataTable();
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
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
                Dictionary<string, NpgsqlDbType> result = new Dictionary<string, NpgsqlDbType>(count);
                string strfieldname;
                string strtype;
                
                NpgsqlDbType fieldType = NpgsqlDbType.Varchar;
                for (int i = 0; i < count; i++)
                {
                    DataRow dRow = dt.Rows[i];
                    if (dRow["columnname"] == null || string.IsNullOrWhiteSpace(dRow["columnname"].ToString()) || dRow["typename"] == null || string.IsNullOrWhiteSpace(dRow["typename"].ToString()))
                    {
                        i++;
                        continue;
                    }
                    strtype = dRow["typename"].ToString();
                    strfieldname = dRow["columnname"].ToString();
                    try
                    {
                        switch (strtype.ToLower().Replace("\"",""))
                        {
                            //case "abstime":
                            //    fieldType = NpgsqlDbType.Abstime;
                            //    break;
                            case "bigint":
                                fieldType = NpgsqlDbType.Bigint;
                                break;
                            case "bit":
                                fieldType = NpgsqlDbType.Bit;
                                break;
                            case "boolean":
                                fieldType = NpgsqlDbType.Boolean;
                                break;
                            case "box":
                                fieldType = NpgsqlDbType.Box;
                                break;
                            case "bytea":
                                fieldType = NpgsqlDbType.Bytea;
                                break;
                            case "char":
                            case "character":
                                fieldType = NpgsqlDbType.Char;
                                break;
                            case "circle":
                                fieldType = NpgsqlDbType.Circle;
                                break;
                            case "date":
                                fieldType = NpgsqlDbType.Date;
                                break;
                            case "double precision":
                                fieldType = NpgsqlDbType.Double;
                                break;
                            case "inet":
                                fieldType = NpgsqlDbType.Inet;
                                break;
                            case "integer":
                                fieldType = NpgsqlDbType.Integer;
                                break;
                            case "interval":
                                fieldType = NpgsqlDbType.Interval;
                                break;
                            case "json":
                                fieldType = NpgsqlDbType.Json;
                                break;
                            case "jsonb":
                                fieldType = NpgsqlDbType.Jsonb;
                                break;
                            case "line":
                                fieldType = NpgsqlDbType.Line;
                                break;
                            case "lseg":
                                fieldType = NpgsqlDbType.LSeg;
                                break;
                            case "macaddr":
                            case "macaddr8":
                                fieldType = NpgsqlDbType.MacAddr;
                                break;
                            case "money":
                                fieldType = NpgsqlDbType.Money;
                                break;
                            case "name":
                                fieldType = NpgsqlDbType.Name;
                                break;
                            case "numeric":
                                fieldType = NpgsqlDbType.Numeric;
                                break;
                            case "oidvector":
                                fieldType = NpgsqlDbType.Oidvector;
                                break;
                            case "path":
                                fieldType = NpgsqlDbType.Path;
                                break;
                            case "point":
                                fieldType = NpgsqlDbType.Point;
                                break;
                            case "polygon":
                                fieldType = NpgsqlDbType.Polygon;
                                break;
                            case "real":
                                fieldType = NpgsqlDbType.Real;
                                break;
                            case "refcursor":
                                fieldType = NpgsqlDbType.Refcursor;
                                break;
                            case "smallint":
                                fieldType = NpgsqlDbType.Smallint;
                                break;
                            case "text":
                                fieldType = NpgsqlDbType.Text;
                                break;
                            case "time without time zone":
                                fieldType = NpgsqlDbType.Time;
                                break;
                            case "time with time zone":
                                fieldType = NpgsqlDbType.TimeTz;
                                break;
                            case "timestamp without time zone":
                                fieldType = NpgsqlDbType.Timestamp;
                                break;
                            case "timestamp with time zone":
                                fieldType = NpgsqlDbType.TimestampTz;
                                break;
                            case "uuid":
                                fieldType = NpgsqlDbType.Uuid;
                                break;
                            case "character varying":
                                fieldType = NpgsqlDbType.Varchar;
                                break;
                            case "xml":
                                fieldType = NpgsqlDbType.Xml;
                                break;
                            default:
                                fieldType = NpgsqlDbType.Varchar;
                                break;
                        }

                    }
                    catch (ArgumentException ex)
                    {
                        SystemLogger.getLogger().Warn("获取NpgsqlDbType错误!", ex);
                    }
                    result.Add(strfieldname, fieldType);
                }
                return result;
            }
            return null;
        }

        public bool executeTransaction(System.Data.Common.DbCommand dbCmd)
        {
            NpgsqlCommand cmd = (NpgsqlCommand)dbCmd;
            if (cmd == null || cmd.Connection == null)
            {
                SystemLogger.getLogger().Error("执行SQL错误:NpgsqlCommand为空！");
                return false;
            }
            cmd.CommandType = CommandType.Text;
            NpgsqlTransaction transaction = null;

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
                //if (transaction != null)
                //    transaction.Rollback();
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
            //throw new NotImplementedException();
        }

        public string insertByParamsReturnSQL(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false)
        {
            if (fieldValueDiction.Count > 0)
            {
                if (fieldValueDiction.Count > 0)
                {
                    //数据库查询
                    NpgsqlConnection dbConn = this.open() as NpgsqlConnection;
                    NpgsqlCommand cmd = dbConn.CreateCommand();
                    Dictionary<string, NpgsqlDbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into ");
                    sb.Append(tabName);

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
                    sb.Append(" values('");
                    int i = 0;
                    foreach (string fieldKey in fieldValueDiction.Keys)
                    {
                        if (!fieldTypeDiction.ContainsKey(fieldKey))
                            continue;
                        object tmpValue = fieldValueDiction[fieldKey];
                        if (tmpValue == null)
                            continue;
                        if (i > 0)
                        {
                            sb.Append(",'");
                        }
                        string tmpField = fieldValueDiction[fieldKey].ToString();

                        sb.Append(tmpField);
                        sb.Append("'");
                        i++;
                    }
                    sb.Append(" )");
                    string sql = sb.ToString();
                    sb.Clear();
                    return sql;
                }
            }
            return "";
        }

        public string updateByParamsReturnSQL(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string useField = "")
        {
            if (fieldValueDiction.Count > 0)
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("update ");
                sb.Append(tabName);
                sb.Append(" set ");
                int i = 0;
                string keyFieldName = "";
                object keyFieldValue = null;

                if (string.IsNullOrWhiteSpace(useField))
                {
                    //查询表的主键
                    string selectsql = $@"SELECT
                        pg_attribute.attname AS colname
                    FROM
                        pg_constraint
                    INNER JOIN pg_class ON pg_constraint.conrelid = pg_class.oid
                    INNER JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid
                    AND pg_attribute.attnum = pg_constraint.conkey[1]
                    WHERE
                        pg_class.relname = '{tabName}'
                    AND pg_constraint.contype = 'p'";
                    keyFieldName = executeScalar(selectsql) + "";
                }
                else
                {
                    keyFieldName = useField;
                }

                foreach (string fieldKey in fieldValueDiction.Keys)
                {
                    if (keyFieldName.ToLower() == fieldKey.ToLower())
                    {
                        keyFieldValue = fieldValueDiction[fieldKey];
                        continue;
                    }
                    object tmpValue = fieldValueDiction[fieldKey];
                    if (i > 0)
                    {
                        sb.Append(",");
                    }
                    sb.Append(fieldKey);
                    sb.Append("='");
                    sb.Append(tmpValue.ToString());
                    sb.Append("'");
                    i++;
                }
                sb.Append(" where ");
                sb.Append(keyFieldName);
                sb.Append(" = ");
                sb.Append("'");
                sb.Append(keyFieldValue);
                sb.Append("'");
                string sql = sb.ToString();
                sb.Clear();
                //清空释放内存对象
                if (releaseObj)
                {
                    fieldValueDiction.Clear();
                    fieldValueDiction = null;
                }
                
                return sql;
            }
            return "";
        }
    }
}
