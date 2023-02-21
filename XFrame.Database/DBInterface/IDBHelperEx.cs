using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace XFrame.Database.DBInterface
{
    /// <summary>
    /// 数据库接口扩展 修改为字段为空还增加SQL字段值得情况  
    /// hnayx 2018-1-26
    /// </summary>
    public interface IDBHelperEx : IDBHelper
    {
        /// <summary>
        /// 批量插入   字段空字符串可以插入 
        /// </summary>
        /// <param name="tabName"></param>
        /// <param name="fieldValueDiction"></param>
        /// <param name="releaseObj"></param>
        /// <returns></returns>
        string insertByParamsReturnSQLEx(string tabName, IDictionary<string, object> fieldValueDiction, bool releaseObj = false);
        /// <summary>
        /// 批量插入多个表和更新表 通过事物  
        /// </summary>
        /// <param name="tabNameList">插入修改表的集合</param>
        /// <param name="fieldValueDictionList">插入修改表的字典</param>
        /// <param name="frontSqlList">前置sql集合</param>
        /// <param name="backSqlList">后置sql集合</param>
        /// <param name="updateTabIntList">修改表对应的值为1 否则为0  并且此数组与表的数据个数相等 默认可以不传</param>
        /// <returns></returns>
        bool executeTransactionList(List<string> tabNameList, List<IDictionary<string, object>> fieldValueDictionList, List<string> frontSqlList = null, List<string> backSqlList = null, List<int> updateTabIntList = null);
    }
}
