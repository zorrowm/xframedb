using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace XFrame.Database.DBInterface
{
    /// <summary>
    /// 数据库接口
    /// </summary>
    public interface IDBHelper
    {
        /// <summary>
        /// 初始化连接字符串
        /// </summary>
        /// <param name="connectionString"></param>
        void initConnectString(string connectionString);

        /// <summary>
        /// 连接数据库
        /// </summary>
        /// <param name="connectionString">数据库连接</param>
        /// <returns></returns>
        DbConnection open(string connectionString=null);

        /// <summary>
        /// 执行数据库命令（插入、更新、删除等）
        /// </summary>
        /// <param name="strSQL">SQL命令行</param>
        /// <returns>影响的行数</returns>
        int execute(string strSQL);



        /// <summary>
        /// 获取记录数目
        /// </summary>
        /// <param name="tabName">表名</param>
        /// <returns></returns>
        int getCountByTable(string tabName);


        /// <summary>
        /// 获取记录数目
        /// </summary>
        /// <param name="sql">sql语句</param>
        /// <returns></returns>
        int getCount(string sql);


        /// <summary>
        /// 返回数据集合
        /// </summary>
        /// <param name="strSQL">SQL语句</param>
        /// <returns></returns>
        DataSet executeDataSet(string strSQL);

        /// <summary>
        /// 返回结果集中的第一行第一列，忽略其他行或列
        /// </summary>
        /// <param name="strSQL"></param>
        /// <returns></returns>
        object executeScalar(string strSQL);

        /// <summary>
        /// 返回表记录数据
        /// </summary>
        /// <param name="strSQL"></param>
        /// <returns></returns>
        List<object[]> executeListObjects(string strSQL);

        /// <summary>
        /// 返回单个字段的表记录数据
        /// </summary>
        /// <param name="strSQL"></param>
        /// <returns></returns>
        List<string> executeSingleFieldValueList(string strSQL);

        /// <summary>
        /// 事务方式，执行一组SQL语句
        /// </summary>
        /// <param name="sqlList"></param>
        /// <returns></returns>
        bool executeTransactionSQLList(List<string> sqlList);

        /// <summary>
        /// 事务方式，执行SQL语句
        /// </summary>
        /// <returns></returns>
        bool executeTransaction(string strSQL);

        /// <summary>
        /// 事务方式，执行SQL语句
        /// </summary>
        /// <param name="dbCmd"></param>
        /// <returns></returns>
       bool executeTransaction(DbCommand dbCmd);

        /// <summary>
        /// 以表格形式返回结果
        /// </summary>
        /// <param name="sql">查询语句</param>
        /// <returns></returns>
        DataTable getDataTableResult(string sql);

        /// <summary>
        /// 返回数据读取对象
        /// </summary>
        /// <param name="strSQL">SQL语句</param>
        /// <returns></returns>
        DbDataReader executeDataReader(string strSQL);


        /// <summary>
        /// 通过键值对执行插入语句
        /// </summary>
        /// <param name="tabName">表名</param>
        /// <param name="fieldValueDiction">键值</param>
        /// <param name="releaseObj">是否释放内存对象</param>
        /// <param name="useField">是否使用字段方式</param>
        bool insertByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, bool useField = true);

        /// <summary>
        /// 插入数据时，返回自增列的值
        /// </summary>
        /// <param name="tabName">表名</param>
        /// <param name="fieldValueDiction">键值</param>
        /// <param name="releaseObj">是否释放内存对象</param>
        /// <param name="sequenceSQL">查询序列的语句</param>
        /// <returns>返回自增列的值</returns>
        long insertByParamsReturnSequence(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string sequenceSQL = null);

        /// <summary>
        /// 通过字段键值对,返回插入SQL语句
        /// </summary>
        /// <param name="tabName"></param>
        /// <param name="fieldValueDiction"></param>
        /// <param name="releaseObj">是否释放内存对象</param>
        /// <param name="useField"></param>
        /// <returns></returns>
        string insertByParamsReturnSQL(string tabName,IDictionary<string, object> fieldValueDiction, bool releaseObj = false);

        /// <summary>
        /// 通过键值对执行更新
        /// </summary>
        /// <param name="tabName">表名</param>
        /// <param name="fieldValueDiction">键值</param>
        /// <param name="releaseObj">是否释放内存对象</param>
        /// <param name="useField">更新所依据的字段</param>
        /// <returns></returns>
        bool updateByParams(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string useField = "");

        /// <summary>
        /// 通过字段键值对,返回更新SQL语句
        /// </summary>
        /// <param name="tabName"></param>
        /// <param name="fieldValueDiction"></param>
        /// <param name="releaseObj">是否释放内存对象</param>
        /// <param name="useField"></param>
        /// <returns></returns>
        string updateByParamsReturnSQL(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false, string useField ="");
    }
}
