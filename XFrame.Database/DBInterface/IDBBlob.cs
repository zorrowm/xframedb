using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace XFrame.Database.DBInterface
{
    /// <summary>
    /// 数据库大字段操作接口
    /// </summary>
    public interface IDBBlob
    {
        /// <summary>
        /// 从数据库表中读取大字段
        /// </summary>
        /// <param name="tabName">表名</param>
        /// <param name="blobField">大字段名</param>
        /// <returns></returns>
        byte[] readBlobContent(string tabName, string blobField, string clause);

        /// <summary>
        /// 向大字段中写入数据
        /// </summary>
        /// <param name="tabName">表名</param>
        /// <param name="blobField">大字段名</param>
        /// <param name="contentBuffer">内容缓存</param>
        /// <returns></returns>
        bool writeBlobContent(string tabName, string blobField, byte[] contentBuffer, string clause);
    }
}
