using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.Common;
using System.Globalization;
using MySql.Data.MySqlClient;
using XFrame.Database.DBInterface;
using XFrame.Database.Log;

namespace XFrameDB.MySQL
{
    public class MySQLDBHelper : IDBHelper, IDBBlob, IDBTHelper, IDBHelperEx
    {
        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        private string dbConnString = null;

        public MySQLDBHelper()
        {
        }
        public MySQLDBHelper(string strcon)
        {
            dbConnString = strcon;
        }
        /// <summary>
        /// 初始化连接字符串
        /// </summary>
        /// <param name="connectionString"></param>
        public void initConnectString(string connectionString)
        {
            this.dbConnString = connectionString;

        }

        public DbConnection open(string connectionString = null)
        {

            MySqlConnection dbConn = null;
            if (!string.IsNullOrEmpty(connectionString))
            {
                this.dbConnString = connectionString;
            }
            dbConn = new MySqlConnection(this.dbConnString);
            try
            {
                dbConn.Open();
            }
            catch (Exception ex)
            {
                //if (dbConn.State == ConnectionState.Closed || dbConn.State == ConnectionState.Broken)
                //{
                //    dbConn.Open();
                //}
                //throw new Exception("数据库连接失败", ex);
                throw new Exception("数据库连接失败", ex);
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
            MySqlConnection dbConn = this.open() as MySqlConnection;
            MySqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            MySqlTransaction transaction = dbConn.BeginTransaction();
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
                throw new Exception("执行SQL错误：" + strSQL, ex);
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
            return getCount(sql);
        }

        public int getCount(string sql)
        {
            object value = executeScalar(sql);
            return value == null ? 0 : int.Parse(value.ToString());
        }

        public System.Data.DataSet executeDataSet(string strSQL)
        {
            MySqlConnection dbConn = this.open() as MySqlConnection;
            MySqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            MySqlDataAdapter da = new MySqlDataAdapter(cmd);
            DataSet ds = null;
            try
            {
                ds = new DataSet();
                da.Fill(ds);
            }
            catch (Exception ex)
            {
                throw new Exception("执行SQL错误：" + strSQL, ex);
            }
            finally
            {
                //字段都改成大写
                if (ds != null)
                {
                    foreach (DataTable dt in ds.Tables)
                    {
                        for (int i = 0; i < dt.Columns.Count; i++)
                        {
                            dt.Columns[i].ColumnName = dt.Columns[i].ColumnName.ToUpper();
                        }
                    }
                }
                da.Dispose();
                dbConn.Close();
                dbConn.Dispose();
            }
            return ds;
        }

        public object executeScalar(string strSQL)
        {
            MySqlConnection dbConn = this.open() as MySqlConnection;
            MySqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            try
            {
                return cmd.ExecuteScalar();
            }
            catch (Exception ex)
            {
                throw new Exception("执行SQL错误：" + strSQL, ex);
            }
            finally
            {
                dbConn.Close();
                dbConn.Dispose();
            }
            //return null;
        }

        public List<object[]> executeListObjects(string strSQL)
        {
            MySqlConnection dbConn = this.open() as MySqlConnection;
            MySqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            MySqlDataReader reader = null;
            List<object[]> listObjects = null;
            try
            {
                reader = cmd.ExecuteReader();
                if (reader != null && reader.HasRows)
                {
                    int fieldCount = reader.FieldCount;
                    //int rowCount = (int)reader.RowSize;
                    //listObjects = new List<object[]>(rowCount);
                    listObjects = new List<object[]>();
                    while (reader.Read())
                    {
                        object[] fieldValues = new object[fieldCount];
                        for (int i = 0; i < fieldCount; i++)
                        {
                            //Type fieldType = reader.GetFieldType(i);
                            //if (fieldType == Type.GetType("System.DateTime"))
                            //    fieldValues[i] = reader.GetString(i);//.GetDateTime(i);
                            //else
                                fieldValues[i] = reader.GetValue(i);
                        }
                        listObjects.Add(fieldValues);
                    }
                    return listObjects;
                }
            }
            catch (Exception ex)
            {
                throw new Exception("执行SQL错误" + strSQL, ex);
                //return null;
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
            MySqlConnection dbConn = this.open() as MySqlConnection;
            MySqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            MySqlDataReader reader = null;
            List<string> listObjects = null;
            try
            {
                reader = cmd.ExecuteReader();

                if (reader != null && reader.HasRows)
                {
                    //int rowCount = (int)reader.RowSize;
                    //listObjects = new List<string>(rowCount);
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
                throw new Exception("执行SQL错误" + strSQL, ex);
            }
            finally
            {
                if (reader != null)
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
            MySqlConnection dbConn = this.open() as MySqlConnection;
            MySqlTransaction transaction = dbConn.BeginTransaction();
            MySqlCommand cmd = dbConn.CreateCommand();
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
                throw new Exception("执行SQL错误：" + cmd.CommandText, ex);
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
            MySqlConnection dbConn = this.open() as MySqlConnection;

            MySqlCommand cmd = dbConn.CreateCommand();
            if (cmd == null || cmd.Connection == null)
            {
                throw new Exception("执行SQL错误:OracleCommand为空！");
                //return false;
            }
            cmd.CommandType = CommandType.Text;
            MySqlTransaction transaction = null;
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
                throw new Exception("执行SQL错误", ex);
            }
            finally
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                    transaction.Dispose();
                }
            }
           // return false;
        }
        public bool executeTransactionList(IList<MySqlCommand> dbCmdList, MySqlTransaction transaction)
        {
            try
            {
                for (int i = 0; i < dbCmdList.Count; i++)
                {
                    if (dbCmdList[i].ExecuteNonQuery() < 0)
                    {
                        transaction.Rollback();
                        return false;
                    };//.ExecuteNonQuery();

                }

                transaction.Commit();

                transaction.Dispose();
                transaction = null;
                return true;
            }
            catch (Exception ex)
            {
                if (transaction != null)
                    transaction.Rollback();
                throw new Exception("执行SQL错误", ex);
            }
            finally
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                    transaction.Dispose();
                }
            }
           // return false;
        }
        public bool executeTransaction(DbCommand dbCmd)
        {
            MySqlCommand cmd = (MySqlCommand)dbCmd;
            if (cmd == null || cmd.Connection == null)
            {
                throw new Exception("执行SQL错误:OracleCommand为空！");
                //return false;
            }
            cmd.CommandType = CommandType.Text;
            MySqlTransaction transaction = null;
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
                throw new Exception("执行SQL错误", ex);
            }
            finally
            {
                if (transaction != null)
                {
                    transaction.Rollback();
                    transaction.Dispose();
                }
            }
            //return false;
        }
        /// <summary>
        /// 以表格形式返回结果
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public DataTable getDataTableResult(string sql)
        {
            //数据库查询
            MySqlConnection dbConn = this.open() as MySqlConnection;

            MySqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = sql;
            MySqlDataAdapter da = new MySqlDataAdapter(cmd);
            DataTable dt = null;
            da.ReturnProviderSpecificTypes = false;
            try
            {
                //DataTable dt = new DataTable();
                //da.Fill(dt);
                DataSet ds = new DataSet("test");
                da.Fill(ds);
                dt = ds.Tables[0];
                //if (dt != null)
                //{
                //    bool needConvert = false;
                //    for (int i = 0; i < dt.Columns.Count; i++)
                //    {
                //        Type typeName = dt.Columns[i].DataType;
                //        string strTypeName = typeName.ToString();
                //        if (strTypeName == "MySql.Data.Types.MySqlDateTime")
                //        {
                //            needConvert = true;
                //        }
                //        dt.Columns[i].ColumnName = dt.Columns[i].ColumnName.ToUpper();
                //    }
                //    if (needConvert)
                //    {
                //        //克隆表结构
                //        DataTable dtResult = new DataTable();
                //        dtResult = dt.Clone();
                //        foreach (DataColumn col in dtResult.Columns)
                //        {
                //            if (col.DataType.ToString() == "MySql.Data.Types.MySqlDateTime")
                //            {
                //                //修改列类型
                //                col.DataType = typeof(System.DateTime);
                //            }
                //        }
                //        foreach (DataRow row in dt.Rows)
                //        {
                //            DataRow rowNew = dtResult.NewRow();
                //            foreach (DataColumn col in dtResult.Columns)
                //            {
                //                if (col.DataType.ToString() == "System.DateTime")
                //                {
                //                    if (row[col.ColumnName] != DBNull.Value && row[col.ColumnName] != null)
                //                    {
                //                        string strVal = row[col.ColumnName].ToString();
                //                        if (!strVal.StartsWith("0000"))
                //                        {
                //                            try
                //                            {
                //                                rowNew[col.ColumnName] = Convert.ToDateTime(row[col.ColumnName]);
                //                            }
                //                            catch
                //                            {
                //                                rowNew[col.ColumnName] = DBNull.Value;
                //                            }
                //                        }
                //                        else
                //                            rowNew[col.ColumnName] = DBNull.Value;
                //                    }
                //                    else
                //                        rowNew[col.ColumnName] = DBNull.Value;
                //                }
                //                else
                //                {
                //                    rowNew[col.ColumnName] = row[col.ColumnName];
                //                }
                //            }
                //            dtResult.Rows.Add(rowNew);
                //        }
                //        da.Dispose();
                //        dbConn.Close();
                //        dbConn.Dispose();
                //        return dtResult;
                //    }
                //}
            }
            catch (Exception ex)
            {
                throw new Exception("执行SQL错误：" + sql, ex);
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
            MySqlConnection dbConn = this.open() as MySqlConnection;
            MySqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            MySqlDataReader reader = cmd.ExecuteReader();
            return reader;
        }

        public bool insertByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, bool useField = true)
        {
            bool blexecuted = false;
            if (fieldValueDiction.Count > 0)
            {
                //数据库查询
                MySqlConnection dbConn = this.open() as MySqlConnection;
                MySqlCommand cmd = dbConn.CreateCommand();
                Dictionary<string, MySqlDbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
                StringBuilder sb = new StringBuilder();
                sb.Append("insert into ");
                sb.Append(tabName);

                if (useField)
                {
                    sb.Append("(");
                    bool isFirst = true;
                    foreach (string field in fieldValueDiction.Keys)
                    {
                        if (!fieldTypeDiction.ContainsKey(field))
                            continue;

                        object tmpValue = fieldValueDiction[field];
                        if (tmpValue == null || string.IsNullOrEmpty(tmpValue.ToString()))
                            continue;
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
                    if (!fieldTypeDiction.ContainsKey(fieldKey))
                        continue;
                    object tmpValue = fieldValueDiction[fieldKey];
                    if (tmpValue == null || string.IsNullOrEmpty(tmpValue.ToString()))
                        continue;
                    if (i > 0)
                    {
                        sb.Append(",");
                    }
                    string tmpField = "?" + fieldKey;
                    i++;
                    sb.Append(tmpField);
                    if (fieldTypeDiction != null && fieldTypeDiction.Count > 0)
                    {
                        MySqlDbType tmpDBType = fieldTypeDiction[fieldKey];
                        if (tmpDBType == MySqlDbType.DateTime)
                        {
                            //DateTime dtime = DateTime.ParseExact(tmpValue.ToString(), "yyyy-MM-dd",null);
                            DateTimeFormatInfo dtFormat = new DateTimeFormatInfo();
                            dtFormat.ShortDatePattern = "yyyy/MM/dd";
                            DateTime dtime = Convert.ToDateTime(tmpValue.ToString(), dtFormat);
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = dtime;
                        }
                        else if (tmpDBType == MySqlDbType.Int32)
                        {
                            int intvalue = Convert.ToInt32(tmpValue.ToString());
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = intvalue;
                        }
                        else if (tmpDBType == MySqlDbType.Double)
                        {
                            double doublevalue = Convert.ToDouble(tmpValue.ToString());
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = doublevalue;
                        }
                        else if (tmpDBType == MySqlDbType.Decimal)
                        {
                            decimal decimalvalue = Convert.ToDecimal(tmpValue.ToString());
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = decimalvalue;
                        }
                        else
                        {
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = tmpValue;
                        }
                    }
                    else
                    {
                        cmd.Parameters.Add(tmpField, MySqlDbType.String).Value = tmpValue;
                    }
                }
                sb.Append(" )");
                string sql = sb.ToString();
                sb.Clear();
                //SQL语句
                cmd.CommandText = sql;
                //清空
                //fieldTypeDiction.Clear();
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

        public bool updateByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string useField = "")
        {
            bool blexecuted = false;
            if (fieldValueDiction.Count > 0)
            {
                //数据库查询
                MySqlConnection dbConn = this.open() as MySqlConnection;
                MySqlCommand cmd = dbConn.CreateCommand();
                Dictionary<string, MySqlDbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
                StringBuilder sb = new StringBuilder();
                sb.Append("update ");
                sb.Append(tabName);
                sb.Append(" set ");
                int i = 0;
                string keyFieldName = "";
                object keyFieldValue = null;

                if (string.IsNullOrEmpty(useField))
                {
                    //查询获取数据库表的主键
                    string selectsql = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_name='" + tabName + "' AND COLUMN_KEY='PRI'";
                    keyFieldName = executeScalar(selectsql).ToString();
                }
                else
                {
                    keyFieldName = useField;
                }

                foreach (string fieldKey in fieldValueDiction.Keys)
                {
                    if (!fieldTypeDiction.ContainsKey(fieldKey))
                        continue;

                    if (keyFieldName.ToUpper() == fieldKey.ToUpper())
                    {
                        keyFieldValue = fieldValueDiction[fieldKey];
                        continue;
                    }
                    object tmpValue = fieldValueDiction[fieldKey];
                    ////为空 则不更新此字段；即认为空字段是不更新字段
                    //if (tmpValue == null)
                    //    continue;
                    if (i > 0)
                    {
                        sb.Append(",");
                    }
                    string tmpField = "?" + fieldKey;
                    i++;
                    sb.Append(fieldKey);
                    sb.Append("=");
                    sb.Append(tmpField);

                    MySqlDbType tmpDBType = fieldTypeDiction[fieldKey];
                    //MySqlParameter parameters = new MySqlParameter(tmpField, tmpValue);
                    //parameters.MySqlDbType = tmpDBType;
                    //parameters.Direction = ParameterDirection.Input;

                    if (tmpDBType == MySqlDbType.Date)
                    {
                        if (tmpValue == null)
                        {
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = null;
                        }
                        else
                        {
                            DateTimeFormatInfo dtFormat = new DateTimeFormatInfo();
                            dtFormat.ShortDatePattern = "yyyy/MM/dd";
                            DateTime dtime = Convert.ToDateTime(tmpValue.ToString(), dtFormat);
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = dtime;
                        }
                    }
                    else if (tmpDBType == MySqlDbType.Int32)
                    {
                        if (tmpValue == null)
                        {
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = null;
                        }
                        else
                        {
                            int intvalue = Convert.ToInt32(tmpValue.ToString());
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = intvalue;
                        }
                    }
                    else if (tmpDBType == MySqlDbType.Double)
                    {
                        if (tmpValue == null)
                        {
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = null;
                        }
                        else
                        {
                            double doublevalue = Convert.ToDouble(tmpValue.ToString());
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = doublevalue;
                        }
                    }
                    else if (tmpDBType == MySqlDbType.Decimal)
                    {
                        if (tmpValue == null)
                        {
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = null;
                        }
                        else
                        {
                            decimal decimalvalue = Convert.ToDecimal(tmpValue.ToString());
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = decimalvalue;
                        }
                    }
                    else
                    {
                        if (tmpValue == null)
                        {
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = null;
                        }
                        else
                        {
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = tmpValue;
                        }
                    }
                }

                sb.Append(" where ");
                sb.Append(keyFieldName);
                sb.Append(" = ");

                if (fieldTypeDiction != null && fieldTypeDiction.Count > 0)
                {
                    MySqlDbType tmpDBType = fieldTypeDiction[keyFieldName];
                    switch (tmpDBType)
                    {
                        case MySqlDbType.Bit:
                        case MySqlDbType.Int16:
                        case MySqlDbType.Int32:
                        case MySqlDbType.Int64:
                        case MySqlDbType.Decimal:
                        case MySqlDbType.Double:
                            sb.Append(keyFieldValue);
                            break;
                        default:
                            sb.Append("'");
                            sb.Append(keyFieldValue);
                            sb.Append("'");
                            break;
                    }
                }
                else
                {
                    sb.Append("'");
                    sb.Append(keyFieldValue);
                    sb.Append("'");
                }

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
        }

        /// <summary>
        /// 查询表结构
        /// </summary>
        /// <param name="tabName"></param>
        /// <param name="cmd"></param>
        /// <returns></returns>
        private Dictionary<string, MySqlDbType> getTableFieldType(string tabName, MySqlCommand cmd)
        {
            string sql = "show full columns from " + tabName;
            cmd.CommandText = sql;
            DataTable dt = new DataTable();
            MySqlDataAdapter da = new MySqlDataAdapter(cmd);
            try
            {
                da.Fill(dt);
            }
            catch (Exception ex)
            {
                throw new Exception("执行SQL错误：" + sql, ex);
            }
            finally
            {
                da.Dispose();
            }
            int count = dt.Rows.Count;
            if (count > 0)
            {
                Dictionary<string, MySqlDbType> result = new Dictionary<string, MySqlDbType>(count, StringComparer.OrdinalIgnoreCase);
                string strtype;
                string strfieldname;
                MySqlDbType fieldType = MySqlDbType.VarChar;
                for (int i = 0; i < count; i++)
                {
                    DataRow drow = dt.Rows[i];
                    if (drow["Type"] == null || drow["Field"] == null || string.IsNullOrEmpty(drow["Type"].ToString()) || string.IsNullOrEmpty(drow["Field"].ToString()))
                    {
                        i++;
                        continue;
                    }
                    string[] strsplit = drow["Type"].ToString().Split('(');
                    strtype = strsplit[0].ToLower();
                    strfieldname = drow["Field"].ToString();
                    try
                    {
                        switch (strtype)
                        {
                            case "tinyint":
                            case "bit":
                                fieldType = MySqlDbType.Bit;
                                break;
                            case "smallint":
                                fieldType = MySqlDbType.Int16;
                                break;
                            case "mediumint":
                            case "int":
                            case "integer":
                                fieldType = MySqlDbType.Int32;
                                break;
                            case "bigint":
                                fieldType = MySqlDbType.Int64;
                                break;
                            case "real":
                            case "double":
                                fieldType = MySqlDbType.Double;
                                break;
                            case "float":
                                fieldType = MySqlDbType.Float;
                                break;
                            case "decimal":
                            case "numeric":
                                fieldType = MySqlDbType.Decimal;
                                break;
                            case "char":
                            case "varchar":
                                fieldType = MySqlDbType.String;
                                break;
                            case "date":
                                fieldType = MySqlDbType.Date;
                                break;
                            case "time":
                                fieldType = MySqlDbType.Time;
                                break;
                            case "year":
                                fieldType = MySqlDbType.Year;
                                break;
                            case "timestamp":
                                fieldType = MySqlDbType.Timestamp;
                                break;
                            case "datetime":
                                fieldType = MySqlDbType.DateTime;
                                break;
                            case "tinyblob":
                                fieldType = MySqlDbType.TinyBlob;
                                break;
                            case "blob":
                                fieldType = MySqlDbType.Blob;
                                break;
                            case "mediumblob":
                                fieldType = MySqlDbType.MediumBlob;
                                break;
                            case "longblob":
                                fieldType = MySqlDbType.LongBlob;
                                break;
                            case "tinytext":
                                fieldType = MySqlDbType.TinyText;
                                break;
                            case "text":
                                fieldType = MySqlDbType.Text;
                                break;
                            case "mediumtext":
                                fieldType = MySqlDbType.MediumText;
                                break;
                            case "longtext":
                                fieldType = MySqlDbType.LongText;
                                break;
                            case "enum":
                                fieldType = MySqlDbType.Enum;
                                break;
                            case "set":
                                fieldType = MySqlDbType.Set;
                                break;
                            case "binary":
                                fieldType = MySqlDbType.Binary;
                                break;
                            case "varbinary":
                                fieldType = MySqlDbType.VarBinary;
                                break;
                            case "point":
                            case "linestring":
                            case "polygon":
                            case "geometry":
                            case "multipoint":
                            case "multilinestring":
                            case "multipolygon":
                            case "geometrycollection":
                                fieldType = MySqlDbType.Geometry;
                                break;
                            default:
                                fieldType = MySqlDbType.VarChar;
                                break;
                        }
                    }
                    catch (ArgumentException ex)
                    {
                        SystemLogger.getLogger().Warn("获取MySqlDbType错误!", ex);
                        i++;
                        continue;
                    }
                    result.Add(strfieldname, fieldType);
                }
                return result;
            }
            return null;
        }

        public byte[] readBlobContent(string tabName, string blobField, string clause)
        {
            MySqlConnection dbConn = this.open() as MySqlConnection;
            MySqlCommand cmd = dbConn.CreateCommand();
            string cmdText = string.Format("select {0} from {1} where {2}", tabName, blobField, clause);
            cmd.CommandText = cmdText;
            MySqlDataReader reader = cmd.ExecuteReader();
            long blobDataSize = 0; //BLOB数据体实际大小
            long readStartByte = 0;//从BLOB数据体的何处开始读取数据
            int bufferStartByte = 0;//将数据从buffer数组的何处开始写入
            int hopeReadSize = 1024; //希望每次从BLOB数据体中读取数据的大小
            long realReadSize = 0;//每次实际从BLOB数据体中读取数据的大小
            //CommandBehavior.SequentialAccess将使OracleDataReader以流的方式加载BLOB数据
            MySqlDataReader dr = cmd.ExecuteReader(CommandBehavior.SequentialAccess);
            byte[] buffer = null;
            try
            {
                while (dr.Read())
                {
                    blobDataSize = dr.GetBytes(0, 0, null, 0, 0); //获取这个BLOB数据体的总大小
                    buffer = new byte[blobDataSize];
                    realReadSize = dr.GetBytes(0, readStartByte, buffer, bufferStartByte, hopeReadSize);
                    //循环，每次读取1024byte大小
                    while ((int)realReadSize == hopeReadSize)
                    {
                        bufferStartByte += hopeReadSize;
                        readStartByte += realReadSize;
                        realReadSize = dr.GetBytes(0, readStartByte, buffer, bufferStartByte, hopeReadSize);
                    }
                    //读取BLOB数据体最后剩余的小于1024byte大小的数据
                    dr.GetBytes(0, readStartByte, buffer, bufferStartByte, (int)realReadSize);
                    //读取完成后，BLOB数据体的二进制数据就转换到这个byte数组buffer上去了
                }
            }
            catch (Exception ex)
            {
                throw new Exception("执行SQL错误" + cmdText, ex);
            }
            finally
            {
                dbConn.Close();
                dbConn.Dispose();
            }

            return buffer;
        }

        public bool writeBlobContent(string tabName, string blobField, byte[] contentBuffer, string clause)
        {
            MySqlConnection dbConn = this.open() as MySqlConnection;
            string strSQL = "UPDATE " + tabName + " SET " + blobField + " =:blob WHERE " + clause;
            MySqlCommand cmd = new MySqlCommand(strSQL, dbConn);
            MySqlTransaction transaction = dbConn.BeginTransaction();
            int result = -1;
            try
            {
                cmd.Transaction = transaction;
                //采用新的方法，AddWithValue();
                //cmd.Parameters.Add("blob", MySqlDbType .Blob, contentBuffer, ParameterDirection.Input);
                cmd.Parameters.AddWithValue("blob", contentBuffer);
                result = cmd.ExecuteNonQuery();
                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                throw new Exception("执行SQL错误", ex);
            }
            finally
            {
                dbConn.Close();
                dbConn.Dispose();
            }
            return result > 0 ? true : false;
        }

        public bool multiInsertTable(DataTable dataTable, string tab_name)
        {
            return false;
        }

        public bool multiUpdateTable(DataTable dataTable, string tab_name, List<string> Columns)
        {
            return false;
        }


        public bool insertTable(DataTable dataTable, string tab_name, Dictionary<string, string> field)
        {
            string sql = string.Format("select * form {0} where 1!=1", tab_name);//获取表的列名即表结构
            using (MySqlConnection conn = this.open() as MySqlConnection)
            {
                try
                {
                    MySqlCommand cmd = new MySqlCommand(sql, conn);
                    MySqlDataAdapter adapter = new MySqlDataAdapter(cmd);
                    MySqlCommandBuilder cb = new MySqlCommandBuilder(adapter);
                    DataTable dsNew = new DataTable();
                    int count = adapter.Fill(dsNew);
                    dataTable.Copy();
                    //bool existZhan = false;
                    //string fieldName = field;
                    //if (dataTable.Columns.Contains(fieldName))
                    //{
                    //    existZhan = true;
                    //}
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        //WM:处理占地面积字段值为空
                        //if (existZhan)
                        //{
                        //    object tmpObj = dataTable.Rows[i][fieldName];
                        //    double result;

                        //    if (tmpObj == null || string.IsNullOrEmpty(tmpObj.ToString()))
                        //    {
                        //        tmpObj = "0";
                        //    }
                        //    bool success = double.TryParse(tmpObj.ToString(), out result);
                        //    if (!success)
                        //    {
                        //        result = 0;
                        //    }
                        //    dataTable.Rows[i][fieldName] = result;
                        //}
                        DataRow dr = dsNew.NewRow();
                        dr.ItemArray = dataTable.Rows[i].ItemArray;
                        dsNew.Rows.Add(dr);
                    }
                    //table整体更新
                    count = adapter.Update(dsNew);
                    adapter.UpdateBatchSize = 1000;
                    dsNew.Dispose();
                    return true;
                }
                catch (Exception e)
                {
                    SystemLogger.getLogger().Error(e);
                    return false;
                }
            }
        }
        public DataTable getTable(string sql, List<Type> fieldType)
        {
            MySqlConnection dbConn = this.open() as MySqlConnection;
            MySqlCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = sql;
            MySqlDataReader reader = cmd.ExecuteReader();
            if (reader.HasRows)
            {
                DataTable dt = new DataTable();
                int fieldCount = reader.FieldCount;
                DataTable dSchema = reader.GetSchemaTable();
                for (int i = 0; i < fieldCount; i++)
                {
                    Type Type = reader.GetFieldType(i);
                    string fName = dSchema.Rows[i][0].ToString();
                    if (fieldType.Contains(Type))
                    {
                        DataColumn dCol = new DataColumn(fName, typeof(String));
                        dt.Columns.Add(dCol);
                    }
                    else
                    {
                        DataColumn dcol = new DataColumn(fName);
                        dt.Columns.Add(dcol);
                    }
                }
                int colCount = dt.Columns.Count;
                while (reader.Read())
                {
                    object[] fieldValues = new object[fieldCount];
                    DataRow drow = dt.NewRow();
                    for (int i = 0; i < fieldCount; i++)
                    {
                        if (!reader.IsDBNull(i))
                        {
                            drow[i] = reader.GetString(i);
                        }
                    }
                    dt.Rows.Add(drow);
                }
                if (dt != null)
                {
                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        dt.Columns[i].ColumnName = dt.Columns[i].ColumnName.ToUpper();
                    }
                }
                return dt;
            }
            return null;
        }

        public DataTable getDataTable(string _sql, out DbDataAdapter _adapter)
        {
            _adapter = this.CreateDataAdapter(_sql);
            if (_adapter == null)
                return null;
            DataTable dt_ = new DataTable("tempTable");
            _adapter.Fill(dt_);

            if (dt_ != null)
            {
                for (int i = 0; i < dt_.Columns.Count; i++)
                {
                    dt_.Columns[i].ColumnName = dt_.Columns[i].ColumnName.ToUpper();
                }
            }
            return dt_;
        }
        /// <summary>
        /// 创建数据适配器
        /// </summary>
        /// <param name="_sql">Sql查询语句</param>
        /// <param name="_conn">数据库连接对象</param>
        /// <returns></returns>
        private MySqlDataAdapter CreateDataAdapter(string _sql)
        {
            MySqlConnection conn = this.open() as MySqlConnection;
            MySqlDataAdapter adapter_ = new MySqlDataAdapter();
            MySqlCommandBuilder dbCommandBuilder_ = new MySqlCommandBuilder(adapter_);
            MySqlCommand selectCommand_ = new MySqlCommand(_sql, conn);
            adapter_.SelectCommand = selectCommand_;
            MySqlCommand insertCommand_ = dbCommandBuilder_.GetInsertCommand();
            MySqlCommand updateCommand_ = dbCommandBuilder_.GetUpdateCommand();
            MySqlCommand deleteCommand_ = dbCommandBuilder_.GetDeleteCommand();
            adapter_.InsertCommand = insertCommand_;
            adapter_.UpdateCommand = updateCommand_;
            adapter_.DeleteCommand = deleteCommand_;
            adapter_.InsertCommand.Connection = conn;
            adapter_.UpdateCommand.Connection = conn;
            adapter_.DeleteCommand.Connection = conn;
            return adapter_;
        }

        public long insertByParamsReturnSequence(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string sequenceSQL = null)
        {
            bool result = insertByParams(tabName, fieldValueDiction, releaseObj);
            if (!result)
                return -1;
            string sql = "select last_insert_id();";
            object seq = executeScalar(sql);
            if (seq == null)
                return -1;
            return long.Parse(seq.ToString());
        }

        public string insertByParamsReturnSQL(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false)
        {
            if (fieldValueDiction.Count > 0)
            {
                //数据库查询
                MySqlConnection dbConn = this.open() as MySqlConnection;
                MySqlCommand cmd = dbConn.CreateCommand();
                Dictionary<string, MySqlDbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
                StringBuilder sb = new StringBuilder();
                sb.Append("insert into ");
                sb.Append(tabName);

                sb.Append("(");
                bool isFirst = true;
                foreach (string field in fieldValueDiction.Keys)
                {
                    if (!fieldTypeDiction.ContainsKey(field))
                        continue;
                    object tmpValue = fieldValueDiction[field];
                    if (tmpValue == null || string.IsNullOrEmpty(tmpValue.ToString()))
                        continue;
                    if (!isFirst)
                    {
                        sb.Append(",");
                    }
                    sb.Append("`" + field + "`");
                    isFirst = false;
                }
                sb.Append(")");

                sb.Append(" values(");
                int i = 0;
                foreach (string fieldKey in fieldValueDiction.Keys)
                {
                    if (!fieldTypeDiction.ContainsKey(fieldKey))
                        continue;
                    object tmpValue = fieldValueDiction[fieldKey];
                    if (tmpValue == null || string.IsNullOrEmpty(tmpValue.ToString()))
                        continue;
                    if (i > 0)
                    {
                        sb.Append(",");
                    }
                    string strvalue = "";
                    if (fieldTypeDiction != null && fieldTypeDiction.Count > 0)
                    {
                        MySqlDbType tmpDBType = fieldTypeDiction[fieldKey];
                        switch (tmpDBType)
                        {
                            case MySqlDbType.Bit:
                            case MySqlDbType.Int16:
                            case MySqlDbType.Int32:
                            case MySqlDbType.Int64:
                            case MySqlDbType.Decimal:
                            case MySqlDbType.Double:

                                strvalue = tmpValue.ToString();
                                break;
                            default:
                                strvalue = "'" + tmpValue.ToString() + "'";
                                break;
                        }
                    }
                    else
                        strvalue = "'" + tmpValue.ToString() + "'";
                    i++;
                    sb.Append(strvalue);
                }
                sb.Append(" )");
                string sql = sb.ToString();
                sb.Clear();
                dbConn.Close();
                dbConn.Dispose();
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

        public string updateByParamsReturnSQL(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string useField = "")
        {
            if (fieldValueDiction.Count > 0)
            {
                //数据库查询
                MySqlConnection dbConn = this.open() as MySqlConnection;
                MySqlCommand cmd = dbConn.CreateCommand();
                Dictionary<string, MySqlDbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
                StringBuilder sb = new StringBuilder();
                sb.Append("update ");
                sb.Append(tabName);
                sb.Append(" set ");
                int i = 0;
                string keyFieldName = "";
                object keyFieldValue = null;

                if (string.IsNullOrEmpty(useField))
                {
                    //查询获取数据库表的主键
                    string selectsql = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_name='" + tabName + "' AND COLUMN_KEY='PRI'";
                    keyFieldName = executeScalar(selectsql).ToString();
                }
                else
                {
                    keyFieldName = useField;
                }

                foreach (string fieldKey in fieldValueDiction.Keys)
                {
                    if (!fieldTypeDiction.ContainsKey(fieldKey))
                        continue;
                    if (keyFieldName.ToUpper() == fieldKey.ToUpper())
                    {
                        keyFieldValue = fieldValueDiction[fieldKey];
                        continue;
                    }
                    object tmpValue = fieldValueDiction[fieldKey];

                    if (i > 0)
                    {
                        sb.Append(",");
                    }
                    i++;
                    sb.Append("`" + fieldKey + "`");
                    sb.Append("=");

                    string strvalue = "";
                    if (fieldTypeDiction != null && fieldTypeDiction.Count > 0)
                    {
                        MySqlDbType tmpDBType = fieldTypeDiction[fieldKey];
                        switch (tmpDBType)
                        {
                            case MySqlDbType.Bit:
                            case MySqlDbType.Int16:
                            case MySqlDbType.Int32:
                            case MySqlDbType.Int64:
                            case MySqlDbType.Decimal:
                            case MySqlDbType.Double:
                                if (tmpValue == null)
                                {
                                    strvalue = "null";
                                }
                                else
                                    strvalue = tmpValue.ToString();
                                break;
                            default:
                                if (tmpValue == null)
                                {
                                    strvalue = "null";
                                }
                                else
                                    strvalue = "'" + tmpValue.ToString() + "'";
                                break;
                        }
                    }
                    else
                    {
                        if (tmpValue == null)
                        {
                            strvalue = "null";
                        }
                        else
                            strvalue = "'" + tmpValue.ToString() + "'";
                    }
                    sb.Append(strvalue);
                }
                sb.Append(" where ");
                sb.Append("`" + keyFieldName + "`");
                sb.Append(" = ");

                if (fieldTypeDiction != null && fieldTypeDiction.Count > 0)
                {
                    MySqlDbType tmpDBType = fieldTypeDiction[keyFieldName];
                    switch (tmpDBType)
                    {
                        case MySqlDbType.Bit:
                        case MySqlDbType.Int16:
                        case MySqlDbType.Int32:
                        case MySqlDbType.Int64:
                        case MySqlDbType.Decimal:
                        case MySqlDbType.Double:
                            sb.Append(keyFieldValue);
                            break;
                        default:
                            sb.Append("'");
                            sb.Append(keyFieldValue);
                            sb.Append("'");
                            break;
                    }
                }
                else
                {
                    sb.Append("'");
                    sb.Append(keyFieldValue);
                    sb.Append("'");
                }
                string sql = sb.ToString();
                sb.Clear();
                dbConn.Close();
                dbConn.Dispose();
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


        public string insertByParamsReturnSQLEx(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false)
        {
            if (fieldValueDiction.Count > 0)
            {
                //数据库查询
                MySqlConnection dbConn = this.open() as MySqlConnection;
                MySqlCommand cmd = dbConn.CreateCommand();
                Dictionary<string, MySqlDbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
                StringBuilder sb = new StringBuilder();
                sb.Append("insert into ");
                sb.Append(tabName);

                sb.Append("(");
                bool isFirst = true;
                foreach (string field in fieldValueDiction.Keys)
                {
                    if (!fieldTypeDiction.ContainsKey(field))
                        continue;
                    object tmpValue = fieldValueDiction[field];
                    if (tmpValue == null)
                        continue;
                    if (!isFirst)
                    {
                        sb.Append(",");
                    }
                    sb.Append("`" + field + "`");
                    isFirst = false;
                }
                sb.Append(")");

                sb.Append(" values(");
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
                        sb.Append(",");
                    }
                    string strvalue = "";
                    if (fieldTypeDiction != null && fieldTypeDiction.Count > 0)
                    {
                        MySqlDbType tmpDBType = fieldTypeDiction[fieldKey];
                        switch (tmpDBType)
                        {
                            case MySqlDbType.Bit:
                            case MySqlDbType.Int16:
                            case MySqlDbType.Int32:
                            case MySqlDbType.Int64:
                            case MySqlDbType.Decimal:
                            case MySqlDbType.Double:

                                strvalue = tmpValue.ToString();
                                break;
                            default:
                                strvalue = "'" + tmpValue.ToString() + "'";
                                break;
                        }
                    }
                    else
                        strvalue = "'" + tmpValue.ToString() + "'";
                    i++;
                    sb.Append(strvalue);
                }
                sb.Append(" )");
                string sql = sb.ToString();
                sb.Clear();
                dbConn.Close();
                dbConn.Dispose();
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

        public bool executeTransactionList(List<string> tabNameList, List<IDictionary<string, object>> fieldValueDictionList, List<string> frontSqlList = null, List<string> backSqlList = null, List<int> updateTabList = null)
        {
            bool blexecuted = false;
            //数据库查询
            MySqlConnection dbConn = this.open() as MySqlConnection;
            var transaction = dbConn.BeginTransaction();
            List<MySqlCommand> cmdList = new List<MySqlCommand>();
            if (frontSqlList != null)
            {
                foreach (var item in frontSqlList)
                {
                    MySqlCommand cmd = dbConn.CreateCommand();
                    cmd.CommandText = item;
                    cmd.Transaction = transaction;
                    cmdList.Add(cmd);
                }
            }
            for (int i = 0; i < tabNameList.Count; i++)
            {
                MySqlCommand cmd = dbConn.CreateCommand();
                cmd = updateTabList != null && updateTabList[i] == 1 ? createMysqlCommandByUpdate(tabNameList[i], fieldValueDictionList[i], cmd) : createMysqlCommandByInsert(tabNameList[i], fieldValueDictionList[i], cmd);
                cmd.Transaction = transaction;
                cmdList.Add(cmd);
            }
            if (backSqlList != null)
            {
                foreach (var item in backSqlList)
                {
                    MySqlCommand cmd = dbConn.CreateCommand();
                    cmd.CommandText = item;
                    cmd.Transaction = transaction;
                    cmdList.Add(cmd);
                }
            }

            blexecuted = executeTransactionList(cmdList, transaction);
            dbConn.Close();
            dbConn.Dispose();
            return blexecuted;
        }

        public MySqlCommand createMysqlCommandByInsert(string tabName, IDictionary<string, object> fieldValueDiction, MySqlCommand cmd)
        {
            Dictionary<string, MySqlDbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
            StringBuilder sb = new StringBuilder();
            sb.Append("insert into " + tabName);
            sb.Append("(");
            bool isFirst = true;
            foreach (string field in fieldValueDiction.Keys)
            {
                if (!fieldTypeDiction.ContainsKey(field))
                    continue;

                object tmpValue = fieldValueDiction[field];
                if (tmpValue == null || string.IsNullOrEmpty(tmpValue.ToString()))
                    continue;
                if (!isFirst)
                {
                    sb.Append(",");
                }
                sb.Append(field);
                isFirst = false;
            }
            sb.Append(") values (");
            int i = 0;
            foreach (string fieldKey in fieldValueDiction.Keys)
            {
                if (!fieldTypeDiction.ContainsKey(fieldKey))
                    continue;
                object tmpValue = fieldValueDiction[fieldKey];
                if (tmpValue == null || string.IsNullOrEmpty(tmpValue.ToString()))
                    continue;
                if (i > 0)
                {
                    sb.Append(",");
                }
                string tmpField = "?" + fieldKey;
                i++;
                sb.Append(tmpField);
                if (fieldTypeDiction != null && fieldTypeDiction.Count > 0)
                {
                    MySqlDbType tmpDBType = fieldTypeDiction[fieldKey];
                    if (tmpDBType == MySqlDbType.DateTime)
                    {
                        //DateTime dtime = DateTime.ParseExact(tmpValue.ToString(), "yyyy-MM-dd",null);
                        DateTimeFormatInfo dtFormat = new DateTimeFormatInfo();
                        dtFormat.ShortDatePattern = "yyyy/MM/dd";
                        DateTime dtime = Convert.ToDateTime(tmpValue.ToString(), dtFormat);
                        cmd.Parameters.Add(tmpField, tmpDBType).Value = dtime;
                    }
                    else if (tmpDBType == MySqlDbType.Int32)
                    {
                        int intvalue = Convert.ToInt32(tmpValue.ToString());
                        cmd.Parameters.Add(tmpField, tmpDBType).Value = intvalue;
                    }
                    else if (tmpDBType == MySqlDbType.Double)
                    {
                        double doublevalue = Convert.ToDouble(tmpValue.ToString());
                        cmd.Parameters.Add(tmpField, tmpDBType).Value = doublevalue;
                    }
                    else if (tmpDBType == MySqlDbType.Decimal)
                    {
                        decimal decimalvalue = Convert.ToDecimal(tmpValue.ToString());
                        cmd.Parameters.Add(tmpField, tmpDBType).Value = decimalvalue;
                    }
                    else
                    {
                        cmd.Parameters.Add(tmpField, tmpDBType).Value = tmpValue;
                    }
                }
                else
                {
                    cmd.Parameters.Add(tmpField, MySqlDbType.String).Value = tmpValue;
                }
            }
            sb.Append(" )");
            string sql = sb.ToString();
            sb.Clear();
            cmd.CommandText = sql;
            fieldTypeDiction.Clear();
            fieldTypeDiction = null;
            return cmd;
        }


        public MySqlCommand createMysqlCommandByUpdate(string tabName, IDictionary<string, object> fieldValueDiction, MySqlCommand cmd)
        {
            try
            {


                Dictionary<string, MySqlDbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
                StringBuilder sb = new StringBuilder();
                sb.Append("update ");
                sb.Append(tabName);
                sb.Append(" set ");
                int i = 0;
                string keyFieldName = "ID";
                object keyFieldValue = null;
                foreach (string fieldKey in fieldValueDiction.Keys)
                {
                    if (!fieldTypeDiction.ContainsKey(fieldKey))
                        continue;

                    if (keyFieldName.ToUpper() == fieldKey.ToUpper())
                    {
                        keyFieldValue = fieldValueDiction[fieldKey];
                        continue;
                    }
                    object tmpValue = fieldValueDiction[fieldKey];
                    ////为空 则不更新此字段；即认为空字段是不更新字段
                    //if (tmpValue == null)
                    //    continue;
                    if (i > 0)
                    {
                        sb.Append(",");
                    }
                    string tmpField = "?" + fieldKey;
                    i++;
                    sb.Append(fieldKey);
                    sb.Append("=");
                    sb.Append(tmpField);

                    MySqlDbType tmpDBType = fieldTypeDiction[fieldKey];
                    //MySqlParameter parameters = new MySqlParameter(tmpField, tmpValue);
                    //parameters.MySqlDbType = tmpDBType;
                    //parameters.Direction = ParameterDirection.Input;

                    if (tmpDBType == MySqlDbType.Date)
                    {
                        if (tmpValue == null)
                        {
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = null;
                        }
                        else
                        {
                            DateTimeFormatInfo dtFormat = new DateTimeFormatInfo();
                            dtFormat.ShortDatePattern = "yyyy/MM/dd";
                            DateTime dtime = Convert.ToDateTime(tmpValue.ToString(), dtFormat);
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = dtime;
                        }
                    }
                    else if (tmpDBType == MySqlDbType.Int32)
                    {
                        if (tmpValue == null)
                        {
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = null;
                        }
                        else
                        {
                            int intvalue = Convert.ToInt32(tmpValue.ToString());
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = intvalue;
                        }
                    }
                    else if (tmpDBType == MySqlDbType.Double)
                    {
                        if (tmpValue == null)
                        {
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = null;
                        }
                        else
                        {
                            double doublevalue = Convert.ToDouble(tmpValue.ToString());
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = doublevalue;
                        }
                    }
                    else if (tmpDBType == MySqlDbType.Decimal)
                    {
                        if (tmpValue == null)
                        {
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = null;
                        }
                        else
                        {
                            decimal decimalvalue = Convert.ToDecimal(tmpValue.ToString());
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = decimalvalue;
                        }
                    }
                    else
                    {
                        if (tmpValue == null)
                        {
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = null;
                        }
                        else
                        {
                            cmd.Parameters.Add(tmpField, tmpDBType).Value = tmpValue;
                        }
                    }
                }

                sb.Append(" where ");
                sb.Append(keyFieldName);
                sb.Append(" = ");

                if (fieldTypeDiction != null && fieldTypeDiction.Count > 0)
                {
                    MySqlDbType tmpDBType = fieldTypeDiction[keyFieldName];
                    switch (tmpDBType)
                    {
                        case MySqlDbType.Bit:
                        case MySqlDbType.Int16:
                        case MySqlDbType.Int32:
                        case MySqlDbType.Int64:
                        case MySqlDbType.Decimal:
                        case MySqlDbType.Double:
                            sb.Append(keyFieldValue);
                            break;
                        default:
                            sb.Append("'");
                            sb.Append(keyFieldValue);
                            sb.Append("'");
                            break;
                    }
                }
                else
                {
                    sb.Append("'");
                    sb.Append(keyFieldValue);
                    sb.Append("'");
                }

                string sql = sb.ToString();
                sb.Clear();
                //SQL语句
                cmd.CommandText = sql;
                //执行SQL语句
                //清空释放内存对象
                fieldValueDiction.Clear();
                return cmd;
            }
            catch (Exception ex)
            {
                SystemLogger.getLogger().Error(ex);
                return null;
            }
        }
    }
}
