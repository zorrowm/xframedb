using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using XFrame.Database.Log;

namespace XFrameDB.Oracle
{
    class OracleTableInsert
    {
        /// <summary>
        /// 同结构表，table整体插入
        /// </summary>
        /// <param name="m_Con">打开的oracle连接</param>
        /// <param name="dataTable">传入的table</param>
        /// <param name="tab_name">表名称</param>
        /// <param name="field">不能为空字段</param>
        /// <returns></returns>        
        public bool insertValueWithDt(OracleConnection m_Con,DataTable dataTable, string tab_name,string field)
        {
            //Open();
            string sql = string.Format("select * form {0} where 1!=1", tab_name);//获取表的列名即表结构
            using (OracleConnection conn = m_Con)
            {
                try
                {
                    conn.Open();
                    OracleCommand cmd = new OracleCommand(sql, conn);
                    OracleDataAdapter adapter = new OracleDataAdapter(cmd);
                    OracleCommandBuilder cb = new OracleCommandBuilder(adapter);
                    DataTable dsNew = new DataTable();
                    int count = adapter.Fill(dsNew);
                    dataTable.Copy();
                    bool existZhan = false;
                    string fieldName = field;
                    if (dataTable.Columns.Contains(fieldName))
                    {
                        existZhan = true;
                    }
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        //WM:处理占地面积字段值为空
                        if (existZhan)
                        {
                            object tmpObj = dataTable.Rows[i][fieldName];
                            double result;

                            if (tmpObj == null || string.IsNullOrEmpty(tmpObj.ToString()))
                            {
                                tmpObj = "0";
                            }
                            bool success = double.TryParse(tmpObj.ToString(), out result);
                            if (!success)
                            {
                                result = 0;
                            }
                            dataTable.Rows[i][fieldName] = result;
                        }
                        DataRow dr = dsNew.NewRow();
                        dr.ItemArray = dataTable.Rows[i].ItemArray;
                        dsNew.Rows.Add(dr);
                    }
                    count = adapter.Update(dsNew);
                    adapter.UpdateBatchSize = 1000;
                    dsNew.Dispose();
                    return true;
                }
                catch (Exception e)
                {
                    SystemLogger.getLogger().Error(e.Message);
                    return false;
                }
            }
        }
        /// <summary>
        /// 获取table，并将所有字段赋值为string类型
        /// </summary>
        /// <param name="cmn">oracle连接</param>
        /// <param name="sql">sql查询语句</param>
        /// <returns></returns>
        public DataTable tableGet(OracleConnection  cmn ,string sql)
        {
            cmn.Open();
            OracleCommand cmd = cmn.CreateCommand();
            cmd.CommandText = sql;
            OracleDataReader reader = cmd.ExecuteReader();
            if(reader .HasRows)
            {
                DataTable dt = new DataTable();
                int fieldCount = reader.FieldCount;
                DataTable dSchema = reader.GetSchemaTable();
                for (int i = 0; i < fieldCount; i++)
                {
                    string fName = dSchema.Rows[i][0].ToString();
                    DataColumn dCol = new DataColumn(fName, typeof(String));
                    dt.Columns.Add(dCol);
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
            return null;       
            cmd.Dispose();
        }

    }
}
