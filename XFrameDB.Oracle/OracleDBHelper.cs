
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using XFrame.Database.Log;
using XFrame.Database.DBInterface;

namespace XFrameDB.Oracle
{
    public class OracleDBHelper:IDBHelper,IDBBlob,IDBTHelper
    {

        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        private string dbConnString = null;
        public OracleDBHelper()
        {
        }
        public OracleDBHelper(string strcon)
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

        public System.Data.Common.DbConnection open(string connectionString=null)
        {
            OracleConnection dbConn = null;
            if (!string.IsNullOrEmpty(connectionString))
            {
                this.dbConnString = connectionString;
            }
            dbConn = new OracleConnection(this.dbConnString);
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
            OracleConnection dbConn = this.open() as OracleConnection;
            OracleCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            OracleTransaction transaction = dbConn.BeginTransaction();
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
            OracleConnection dbConn = this.open() as OracleConnection;
            OracleCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            OracleDataAdapter da = new OracleDataAdapter(cmd);
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
            OracleConnection dbConn = this.open() as OracleConnection;
            OracleCommand cmd = dbConn.CreateCommand();
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
            OracleConnection dbConn = this.open() as OracleConnection;
            OracleCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            OracleDataReader reader = null;
            List<object[]> listObjects = null;
            try
            {
                reader = cmd.ExecuteReader();

                if (reader != null && reader.HasRows)
                {
                    int fieldCount = reader.FieldCount;
                    int rowCount = (int)reader.RowSize;
                    listObjects = new List<object[]>(rowCount);
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
            OracleConnection dbConn = this.open() as OracleConnection;
            OracleCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            OracleDataReader reader = null;
            List<string> listObjects = null;
            try
            {
                reader = cmd.ExecuteReader();

                if (reader != null && reader.HasRows)
                {
                    int rowCount = (int)reader.RowSize;
                    listObjects = new List<string>(rowCount);
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
            OracleConnection dbConn = this.open() as OracleConnection;
            OracleTransaction transaction = dbConn.BeginTransaction();
            OracleCommand cmd = dbConn.CreateCommand();
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
            OracleConnection dbConn = this.open() as OracleConnection;
            OracleCommand cmd = dbConn.CreateCommand();
            if (cmd == null || cmd.Connection == null)
            {
                SystemLogger.getLogger().Error("执行SQL错误:OracleCommand为空！");
                return false;
            }
            cmd.CommandType = CommandType.Text;
            OracleTransaction transaction = null;

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

        public bool executeTransaction(DbCommand dbCmd)
        {
            OracleCommand cmd = (OracleCommand)dbCmd;
            if (cmd == null || cmd.Connection == null)
            {
                SystemLogger.getLogger().Error("执行SQL错误:OracleCommand为空！");
                return false;
            }
            cmd.CommandType = CommandType.Text;
            OracleTransaction transaction = null;

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
            OracleConnection dbConn = this.open() as OracleConnection;
            OracleCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = sql;

            OracleDataAdapter da = new OracleDataAdapter(cmd);

            DataTable dt = null;
            da.ReturnProviderSpecificTypes = false;
            try
            {
                //DataTable dt = new DataTable();
                //da.Fill(dt);
                DataSet ds = new DataSet("test");
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
            OracleConnection dbConn = this.open() as OracleConnection;
            OracleCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = strSQL;
            OracleDataReader reader = cmd.ExecuteReader();
            return reader; 
        }

        public bool insertByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, bool useField = true)
        {
            bool blexecuted = false;
            if (fieldValueDiction.Count > 0)
            {
                //数据库查询
                OracleConnection dbConn = this.open() as OracleConnection;
                OracleCommand cmd = dbConn.CreateCommand();
                Dictionary<string, OracleDbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
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
                        OracleDbType tmpDBType = fieldTypeDiction[fieldKey.ToUpper()];
                        if (tmpDBType == OracleDbType.Date)
                        {
                            //DateTime dtime = DateTime.ParseExact(tmpValue.ToString(), "yyyyMMdd", CultureInfo.CurrentCulture);// DateTime.Parse(tmpValue.ToString(),);
                            ////构造参数
                            //cmd.Parameters.Add(tmpField, tmpDBType, dtime, ParameterDirection.Input);
                            cmd.Parameters.Add(tmpField, tmpDBType, tmpValue, ParameterDirection.Input);
                        }
                        else  //构造参数
                            cmd.Parameters.Add(tmpField, tmpDBType, tmpValue, ParameterDirection.Input);
                    }
                    else
                        //构造参数
                        cmd.Parameters.Add(tmpField, tmpValue);
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

        public bool updateByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string useField = "")
        {
            bool blexecuted = false;
            if (fieldValueDiction.Count > 0)
            {
                //数据库查询
                OracleConnection dbConn = this.open() as OracleConnection;
                OracleCommand cmd = dbConn.CreateCommand();
                Dictionary<string, OracleDbType> fieldTypeDiction = getTableFieldType(tabName, cmd);
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
                    OracleDbType tmpDBType = fieldTypeDiction[fieldKey.ToUpper()];
                    object tmpValue = fieldValueDiction[fieldKey];

                    //为空 则不更新此字段；即认为空字段是不更新字段
                    //if (tmpValue == null)
                    //    continue;

                    if (tmpDBType == OracleDbType.Date)
                    {
                        //DateTime dtime = DateTime.ParseExact(tmpValue.ToString(), "yyyyMMdd", CultureInfo.CurrentCulture);// DateTime.Parse(tmpValue.ToString(),);
                        ////构造参数
                        //cmd.Parameters.Add(tmpField, tmpDBType, dtime, ParameterDirection.Input);
                        cmd.Parameters.Add(tmpField, tmpDBType, tmpValue, ParameterDirection.Input);
                    }
                    else  //构造参数
                        cmd.Parameters.Add(tmpField, tmpDBType, tmpValue, ParameterDirection.Input);
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
        }

        /// <summary>
        /// 查询表结构
        /// </summary>
        /// <param name="tabName"></param>
        /// <param name="cmd"></param>
        /// <returns></returns>
        private Dictionary<string, OracleDbType> getTableFieldType(string tabName, OracleCommand cmd)
        {
            //string sql = "select column_name, data_type,data_precision from user_tab_columns where Table_Name=upper('" + tabName + "')";
            string sql = "select * from " + tabName +" where 1!=1";
            cmd.CommandText = sql;
            DataTable dt = new DataTable();
            OracleDataAdapter da = new OracleDataAdapter(cmd);
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
                Dictionary<string, OracleDbType> result = new Dictionary<string, OracleDbType>(count);
                string field;
                OracleDbType fieldType = OracleDbType.Varchar2;
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
                                    fieldType = OracleDbType.Int32;
                                else
                                    fieldType = OracleDbType.Double;
                                break;
                            //case "DATE":
                            //    fieldType = OracleDbType.TimeStamp;
                            //    break;
                            default:
                                fieldType = (OracleDbType)Enum.Parse(typeof(OracleDbType), type, true);
                                break;
                        }

                    }
                    catch (ArgumentException ex)
                    {
                        SystemLogger.getLogger().Warn("获取OracleDbType错误!", ex);

                    }
                    result.Add(field, fieldType);
                }
                return result;
            }
            return null;
        }

        public byte[] readBlobContent(string tabName, string blobField, string clause)
        {
            OracleConnection dbConn = this.open() as OracleConnection;
            OracleCommand cmd = dbConn.CreateCommand();
            string cmdText = string.Format("select {0} from {1} where {2}", tabName, blobField, clause);
            cmd.CommandText = cmdText;
            OracleDataReader reader = cmd.ExecuteReader();
            long blobDataSize = 0; //BLOB数据体实际大小
            long readStartByte = 0;//从BLOB数据体的何处开始读取数据
            int bufferStartByte = 0;//将数据从buffer数组的何处开始写入
            int hopeReadSize = 1024; //希望每次从BLOB数据体中读取数据的大小
            long realReadSize = 0;//每次实际从BLOB数据体中读取数据的大小
            //CommandBehavior.SequentialAccess将使OracleDataReader以流的方式加载BLOB数据
            OracleDataReader dr = cmd.ExecuteReader(CommandBehavior.SequentialAccess);
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
              SystemLogger.getLogger().Error("执行SQL错误" + cmdText, ex);
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
            OracleConnection dbConn = this.open() as OracleConnection;
            string strSQL = "UPDATE " + tabName + " SET " + blobField + " =:blob WHERE " + clause;
            OracleCommand cmd = new OracleCommand(strSQL, dbConn);
            OracleTransaction transaction = dbConn.BeginTransaction();
            int result = -1;
            try
            {
                cmd.Transaction = transaction;
                //采用新的方法，AddWithValue();
                cmd.Parameters.Add("blob", OracleDbType.Blob, contentBuffer, ParameterDirection.Input);
                result = cmd.ExecuteNonQuery();
                transaction.Commit();

            }
            catch (Exception ex)
            {
                transaction.Rollback();
                SystemLogger.getLogger().Error("执行SQL错误", ex);
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
            string sql = string.Format("select * form {0} where 1!=1", tab_name);//获取表的列名即表结构
            using (OracleConnection conn = this.open() as OracleConnection)
            {
                try
                {
                    OracleCommand cmd = new OracleCommand(sql, conn);
                    OracleDataAdapter adapter = new OracleDataAdapter(cmd);
                    adapter.UpdateBatchSize = 0;
                    OracleCommandBuilder cb = new OracleCommandBuilder(adapter);
                    dataTable.Copy();
                    DataTable dsNew = dataTable.Clone();
                    int times = 0;
                    for (int rowcount = 0; rowcount < dataTable.Rows.Count; times++)
                    {
                        //WM:处理占地面积字段值为空
                        for (int aa = 0; aa < 800 && 800 * times + aa < dataTable.Rows.Count;aa++,rowcount++ )
                        {
                            dsNew.Rows.Add(dataTable.Rows[rowcount].ItemArray);
                        }
                        adapter.Update(dsNew);
                        dsNew.Rows.Clear();
                    }

                    dataTable.Dispose();
                    adapter.Dispose();
                    dsNew.Dispose();
                    return true;
                }
                catch (Exception e)
                {
                    return false;
                }
            }
        }

        public bool multiUpdateTable(DataTable dataTable, string tab_name, List<string>Columns)
        {            
            using (OracleConnection conn = this.open() as OracleConnection)
            {
                string column ="";            
                if(Columns!=null &&Columns .Count >0)
                {
                    foreach(string Column in Columns)
                    {
                        column += Column+",";
                    }
                    column = column .Substring(0,column .Length-1);
                }
                else
                    column = "*";
                try
                {
                    string SQLString = string.Format("select {0} from {1} where rownum=0", column, tab_name);
                    OracleCommand cmd = new OracleCommand(SQLString, conn);
                    OracleDataAdapter myDataAdapter = new OracleDataAdapter();
                    myDataAdapter.SelectCommand = new OracleCommand(SQLString, conn);
                    OracleCommandBuilder custCB = new OracleCommandBuilder(myDataAdapter);
                    custCB.ConflictOption = ConflictOption.OverwriteChanges;
                    custCB.SetAllValues = true;
                    foreach (DataRow dr in dataTable .Rows)
                    {
                        if (dr.RowState == DataRowState.Unchanged)
                            dr.SetModified();
                    }
                    myDataAdapter.Update(dataTable);
                    dataTable.AcceptChanges();
                    myDataAdapter.Dispose();
                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }
        public DataTable getTable(string sql, List<Type> fieldType)
        {
            OracleConnection dbConn = this.open() as OracleConnection;
            OracleCommand cmd = dbConn.CreateCommand();
            cmd.CommandText = sql;
            OracleDataReader reader = cmd.ExecuteReader();
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
                return dt;
            }
            cmd.Dispose(); 
            return null;

        }

        public DataTable getDataTable(string _sql, out DbDataAdapter _adapter)
        {
            _adapter = this.CreateDataAdapter(_sql);
            if (_adapter == null)
                return null;
            DataTable dt_ = new DataTable("tempTable");
            _adapter.Fill(dt_);
            return dt_;
        }

        /// <summary>
        /// 创建数据适配器
        /// </summary>
        /// <param name="_sql">Sql查询语句</param>
        /// <param name="_conn">数据库连接对象</param>
        /// <returns></returns>
        public OracleDataAdapter CreateDataAdapter(string _sql)
        {
            OracleDataAdapter adapter_ = new OracleDataAdapter();
            OracleCommandBuilder dbCommandBuilder_ = new OracleCommandBuilder(adapter_);
            OracleConnection conn = this.open() as OracleConnection;
            OracleCommand selectCommand_ = new OracleCommand(_sql, conn);
            adapter_.SelectCommand = selectCommand_;
            OracleCommand insertCommand_ = dbCommandBuilder_.GetInsertCommand();
            OracleCommand updateCommand_ = dbCommandBuilder_.GetUpdateCommand();
            OracleCommand deleteCommand_ = dbCommandBuilder_.GetDeleteCommand();
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
            bool result = insertByParams(tabName, fieldValueDiction,releaseObj);
            if (!result)
                return -1;
            if (string.IsNullOrEmpty(sequenceSQL))
                return 0;

//就是创建一个SEQUENCE,通过它来获取自增ID， 
//CREATE SEQUENCE MY_TABLE_SEQ; --创建了一个SEQUENCE
//如何用？ 
//插入的时候这样： 
//INSERT INTO MY_TABLE(ID) VALUES(MY_TABLE_SEQ.NEXTVAL);
//            要把当前这个ID返回可以这样： 
//SELECT MY_TABLE_SEQ.CURRVAL FROM DUAL;
            object seq = executeScalar(sequenceSQL);
            if (seq == null)
                return -1;
            return long.Parse(seq.ToString());
        }

        public string insertByParamsReturnSQL(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false)
        {
            throw new NotImplementedException();
        }

        public string updateByParamsReturnSQL(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string useField="")
        {
            throw new NotImplementedException();
        }
    }
}
