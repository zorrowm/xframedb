using System;
using System.Data;
using System.Data.OleDb;

namespace XFrameDB.OleDB
{
    /// <summary>
    /// Excel的工具类
    /// 参看http://developer.51cto.com/art/200907/139788.htm
    /// </summary>
    public class ExcelHelper
    {
        /// <summary>
        /// 导入EXCEL数据为DataTable
        /// </summary>
        /// <param name="path">Excel路径</param>
        /// <returns></returns>
        public static DataTable ExcelToDataTab(string path)  
        {
            string strConn = "Provider=Microsoft.Ace.OleDb.12.0;" + "Data Source=" + path + ";" + "Extended Properties='Excel 12.0;HDR=Yes;IMEX=1';";
            OleDbConnection conn = new OleDbConnection(strConn);
            try
            {
                DataTable dt = new DataTable();
                if (conn.State != ConnectionState.Open)
                    conn.Open();

                DataTable schemaTable = conn.GetOleDbSchemaTable(System.Data.OleDb.OleDbSchemaGuid.Tables, null);
                string tableName = schemaTable.Rows[0][2].ToString().Trim();
                string strExcel = "select * from [" + tableName+"]";
                OleDbDataAdapter adapter = new OleDbDataAdapter(strExcel, conn);
                adapter.Fill(dt);
                return dt;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (conn.State != ConnectionState.Closed)
                    conn.Close();
            }  


            //return null;
        }
        
        public static void DSToExcel(string path,DataSet oldds)   {
            //先得到汇总Excel的DataSet 主要目的是获得Excel在DataSet中的结构             
            string strCon = "Provider=Microsoft.Ace.OleDb.12.0;" + "Data Source=" + path + ";" + "Extended Properties='Excel 12.0;HDR=Yes;IMEX=1';";
            OleDbConnection myConn = new OleDbConnection(strCon) ; 
            //string strCom="select * from [Sheet1$]";   
            myConn.Open ( ) ;
            DataTable schemaTable = myConn.GetOleDbSchemaTable(System.Data.OleDb.OleDbSchemaGuid.Tables, null);
            string tableName = schemaTable.Rows[0][2].ToString().Trim();
            string strCom = "select * from  [" + tableName + "]";
            OleDbDataAdapter myCommand = new OleDbDataAdapter ( strCom, myConn ) ;   
            System.Data.OleDb.OleDbCommandBuilder builder=new OleDbCommandBuilder(myCommand); 
            //QuotePrefix和QuoteSuffix主要是对builder生成InsertComment命令时使用。 
            builder.QuotePrefix="[";     //获取insert语句中保留字符（起始位置）   
            builder.QuoteSuffix="]"; //获取insert语句中保留字符（结束位置）  
            DataSet newds=new DataSet();   
            myCommand.Fill(newds ,"Table1") ;   
            //行数
            int rowCount=oldds.Tables[0].Rows.Count;
            for (int i = 0; i < rowCount; i++)
            {    
                //在这里不能使用ImportRow方法将一行导入到news中，
                //因为ImportRow将保留原来DataRow的所有设置(DataRowState状态不变)。
                //在使用ImportRow后newds内有值，但不能更新到Excel中因为所有导入行的DataRowState!=Added     
                DataRow nrow = newds.Tables["Table1"].NewRow();     
                for(int j=0;j<newds.Tables[0].Columns.Count;j++)    
                {      nrow[j]=oldds.Tables[0].Rows[i][j];     }   
                newds.Tables["Table1"].Rows.Add(nrow);    
            }  
            myCommand.Update(newds,"Table1");
            myConn.Close(); 
        }  
    }
}