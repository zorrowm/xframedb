using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace XFrame.Database.DBInterface
{
    public interface IDBTHelper
    {
        /// <summary>
        /// 以table形式整体插入数据
        /// </summary>
        /// <param name="dataTable">已获取的Datatable数据</param>
        /// <param name="tab_name">更新的表名称</param>
        /// <returns></returns>      
        bool multiInsertTable(DataTable dataTable, string tab_name);

        /// <summary>
        /// table形式批量更新数据
        /// </summary>
        /// <param name="dataTable">要更新的数据集</param>
        /// <param name="tab_name">要更新的表名</param>
        /// <returns></returns>
        bool multiUpdateTable(DataTable dataTable,string tab_name,List<string>Columns);

        /// <summary>
        /// 获取table时对table特使类型字段进行重写
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="fieldType">要更改类型</param>
        /// <returns></returns>
        DataTable getTable(string sql, List<Type>fieldType);

        /// <summary>
        /// 获取数据表
        /// </summary>
        /// <param name="_sql">Sql查询语句</param>
        /// <param name="_adapter">数据适配器</param>
        /// <returns></returns>
        DataTable getDataTable(string _sql, out DbDataAdapter _adapter);
    }

}
