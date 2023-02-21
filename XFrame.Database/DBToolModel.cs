using System.Collections.Generic;
using System.Reflection;
using XFrame.Database.DBInterface;

namespace XFrame.Database
{
    public class DBToolModel
    {
        //数据库配置名
        public string name;
        //反射DLL地址
        public string dbDLLPath;
        //类名
        public string providerName;
        //连接字符串
        public string dbConnString { get; set; }
        
        public Assembly  assembly ;

        public Queue<IDBHelper> dbHelperQueue; 

        public int queueNum { get; set; }
        public DBToolModel(string pname, string pdbDLLName, string pdbConnString,string pproviderName,int pqueueNum=10)
        {
            name = pname;
            dbDLLPath = pdbDLLName;
            dbConnString = pdbConnString;
            providerName=pproviderName;
            dbHelperQueue = new Queue<IDBHelper>(pqueueNum);
            queueNum = pqueueNum;
        }
     }
}