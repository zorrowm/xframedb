using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Reflection;
using System.Text;
using XFrame.Database.DBInterface;

namespace XFrame.Database
{
    /// <summary>
    /// 数据库访问工具
    /// </summary>
    public class ModelDBTool
    {

        /// <summary>
        /// 通过完整SQL语句，从数据库中获取记录，并填充对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dbHelper"></param>
        /// <param name="SQL"></param>
        /// <returns></returns>
        public static List<T> fillModelBySQL<T>(IDBHelper dbHelper, string SQL)
        {
            //查询数据库
            DataTable dt = dbHelper.getDataTableResult(SQL);
            if (dt == null)
                return null;
            int rowCount = dt.Rows.Count;
            if (rowCount > 0)
            {
                List<T> resultModelList = new List<T>(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    DataRow dRow = dt.Rows[i];
                    //实例化视图
                    T instance = fillModelByRow<T>(dRow);
                    //保存到记录列表中
                    resultModelList.Add(instance);
                }
                return resultModelList;
            }
            return null;
        }

        /// <summary>
        /// 从数据库中获取记录，并填充对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dbHelper">数据库连接</param>
        /// <param name="whereClause">查询条件</param>
        /// <returns></returns>
        public static List<T> fillModelByDB<T>(IDBHelper dbHelper,  string whereClause)
        {
            Type classType = typeof(T);
            // type.Name;
            System.Reflection.FieldInfo[] fields = classType.GetFields();
            //表名
            string tabName = classType.Name;
            StringBuilder strBuilder = new StringBuilder();
            //大字段列表
          //  List<FieldInfo> blobFields = null;
            strBuilder.Append("Select ");
            bool isNotFirst = false;
            //分析字段类型
            foreach (System.Reflection.FieldInfo field in fields)
            {
                //是否是大字段
                if (field.FieldType.Equals(typeof(byte[])))
                {
                    //if (blobFields == null)
                    //    blobFields = new List<FieldInfo>();
                    //blobFields.Add(field);
                    continue;
                }
                if (isNotFirst)
                {
                    strBuilder.Append(",");
                }
                strBuilder.Append(field.Name);
                isNotFirst = true;
            }
            strBuilder.Append(" From ");
            strBuilder.Append(tabName);
            strBuilder.Append("  ");
            if(!string.IsNullOrEmpty(whereClause))
                 strBuilder.Append(whereClause);
            string sql = strBuilder.ToString();
            //查询数据库
            DataTable dt = dbHelper.getDataTableResult(sql);
            if (dt == null)
                return null;
            int rowCount = dt.Rows.Count;
            if (rowCount > 0)
            {
                List<T> resultModelList = new List<T>(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    DataRow dRow = dt.Rows[i];
                    //实例化视图
                    T instance = fillModelByRow<T>(dRow);
                    //保存到记录列表中
                    resultModelList.Add( instance);
                }
                return resultModelList;
            }
            return null;
        }


        /// <summary>
        /// 返回单个对象实例
        /// </summary>
        /// <param name="type">类型</param>
        /// <param name="dRow">行记录数</param>
        /// <returns></returns>
        private static T fillModelByRow<T>(DataRow dRow)
        {
            Type type = typeof(T);
            //实例化视图
            T instance = (T)Activator.CreateInstance(type);
             System.Reflection.FieldInfo[] fields = type.GetFields();
             //分析字段类型
            foreach (System.Reflection.FieldInfo fieldInfo in fields)
            {
                bool exist = dRow.Table.Columns.Contains(fieldInfo.Name);
                if(!exist)
                    continue;
                Type dataType = fieldInfo.FieldType;
                object tmpObj = (dRow[fieldInfo.Name] == null ? "" : dRow[fieldInfo.Name]);
                if (!Convert.IsDBNull(tmpObj))
                {
                    if (dataType.Equals(typeof(string)) || dataType.Equals(typeof(Char)))
                    {
                        fieldInfo.SetValue(instance, tmpObj.ToString());
                        continue;
                    }
                    if (dataType.Equals(typeof(int)))
                    {
                        fieldInfo.SetValue(instance, Convert.ToInt32(tmpObj));
                        continue;
                    }
                    if (dataType.Equals(typeof(Single)))//Number(10,2)
                    {
                        fieldInfo.SetValue(instance, Convert.ToSingle(tmpObj));
                        continue;
                    }
                    if (dataType.Equals(typeof(Double)))
                    {
                        fieldInfo.SetValue(instance, Convert.ToDouble(tmpObj));
                        continue;
                    }
                    if (dataType.Equals(typeof(DateTime)))
                    {
                        fieldInfo.SetValue(instance, Convert.ToDateTime(tmpObj));
                        continue;
                    }
                    if (dataType.Equals(typeof(decimal)))//财务上使用
                    {
                        fieldInfo.SetValue(instance, Convert.ToDecimal(tmpObj));
                        continue;
                    }
                    if (dataType.Equals(typeof(double)))
                    {
                        fieldInfo.SetValue(instance, Convert.ToDouble(tmpObj));
                        continue;
                    }
                }
            }
            return instance;
        }

        /// <summary>
        /// 根据字段的中文意思
        /// </summary>
        /// <param name="type"></param>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        public static string getFieldDescrip(Type type,string fieldName)
        {           
            FieldInfo field=type.GetField(fieldName);
            if(field!=null)
            {
                //.net 4.5才能用
                //Attribute attribute = field.GetCustomAttribute(typeof(DescriptionAttribute), false);
                Attribute attribute = Attribute.GetCustomAttribute(field, typeof(DescriptionAttribute));
                DescriptionAttribute dAttribute = attribute as DescriptionAttribute;

                return dAttribute.Description;
            }
            return null;
        }

        /// <summary>
        /// 获取字段列表的中文描述
        /// </summary>
        /// <param name="type"></param>
        /// <param name="fieldName"></param>
        /// <returns></returns>
       public static Dictionary<string,string> getFieldDescripDiction(Type type,string fieldName)
        {           
           // type.Name;
            System.Reflection.FieldInfo[] fields = type.GetFields();
            string fieldDescrip=null;
            Dictionary<string,string> resultDiction=null;
           if(fields.Length>0)
                resultDiction=new Dictionary<string,string>();
            foreach (System.Reflection.FieldInfo field in fields)
            {
                //.net 4.5 以上才能用
                //Attribute attribute = field.GetCustomAttribute(typeof(DescriptionAttribute), false);
                Attribute attribute = Attribute.GetCustomAttribute(field, typeof(DescriptionAttribute));
                DescriptionAttribute dAttribute = attribute as DescriptionAttribute;
                fieldDescrip = dAttribute.Description;
                resultDiction.Add(field.Name, dAttribute.Description);
       
            }
            return resultDiction;
        }



       /// <summary>
       /// 返回单个对象实例
       /// </summary>
       /// <param name="type">类型</param>
       /// <param name="dRow">行记录数</param>
       /// <returns></returns>
       public static bool insertByModel<T>(IDBHelper dbHelper,T model, string keyFieldName)
       {
           Type type = typeof(T);
           //表名
           string tabName = type.Name;
           //属性信息字段
           System.Reflection.FieldInfo[] fields = type.GetFields();

           //大字段列
           List<FieldInfo> blobFieldList = new List<FieldInfo>();
           System.Reflection.FieldInfo keyField =null;
           if(!string.IsNullOrEmpty(keyFieldName))
               keyField = type.GetField(keyFieldName);
           if (keyField == null)
               keyField = fields[0];

           Dictionary<string, object> fieldValueDiction = new Dictionary<string,object>();
           //分析字段类型
           foreach (System.Reflection.FieldInfo fieldInfo in fields)
           {
               Type dataType = fieldInfo.FieldType;
               if (dataType.Equals(typeof(byte[])))
               {
                   blobFieldList.Add(fieldInfo);
                   continue;
               }
               object value= fieldInfo.GetValue(model);
               fieldValueDiction.Add(fieldInfo.Name, value);
           }
           //执行SQL语句
           bool result=dbHelper.insertByParams(tabName, fieldValueDiction);
           if (!result)
           {
               throw new Exception("插入主记录失败！"+model.ToString());
           }

           if (blobFieldList.Count == 0)
           {
               blobFieldList = null;
               return result;
           }
           else if(result)
           {
                IDBBlob dbBlob =(IDBBlob) dbHelper;
                if(dbBlob!=null)
                {
               foreach (FieldInfo tmpFieldInfo in blobFieldList)
               {
                   byte[] contentBuffer=(byte[])tmpFieldInfo.GetValue(model);
                   string keyValue=keyField.GetValue(model).ToString();
                   string whereClause=" where "+keyField.Name+"='"+keyValue+"'";
                    //逐个插入大字段
                   result = dbBlob.writeBlobContent(tabName, tmpFieldInfo.Name, contentBuffer, whereClause);
                   if (!result)
                   {
                            throw new Exception("插入大字段值失败！" + tmpFieldInfo.ToString());
                   }
                }
                }
           }
           return result;
       }

       /// <summary>
       /// 返回单个对象实例
       /// </summary>
       /// <param name="type">类型</param>
       /// <param name="dRow">行记录数</param>
       /// <returns></returns>
       public static bool deleteByModel<T>(IDBHelper dbHelper, T model, string keyFieldName)
       {
           Type type = typeof(T);
           //表名
           string tabName = type.Name;
           System.Reflection.FieldInfo keyField = null;
           if (!string.IsNullOrEmpty(keyFieldName))
               keyField = type.GetField(keyFieldName);
           
           if (keyField != null)
           {
               string keyFieldValue = keyField.GetValue(model).ToString();
               string sql="delete from "+tabName+" where "+keyFieldName+"='"+keyFieldValue+"'";
               //SQL语句
               return dbHelper.execute(sql)>0?true:false;
           }
           return false;
       }

      /// <summary>
      /// 更新模型记录
      /// </summary>
      /// <typeparam name="T"></typeparam>
      /// <param name="model"></param>
      /// <param name="keyFieldName"></param>
      /// <returns></returns>
       public static bool updateByModel<T>(IDBHelper dbHelper, T model, string keyFieldName)
       {
           Type type = typeof(T);
           //表名
           string tabName = type.Name;
           //属性信息字段
           System.Reflection.FieldInfo[] fields = type.GetFields();

           //大字段列
           List<FieldInfo> blobFieldList = new List<FieldInfo>();
           System.Reflection.FieldInfo keyField = null;
           if (!string.IsNullOrEmpty(keyFieldName))
               keyField = type.GetField(keyFieldName);
           if (keyField == null)
               keyField = fields[0];
           string keyValue=keyField.GetValue(model).ToString();
           Dictionary<string, object> fieldValueDiction = new Dictionary<string, object>();
           fieldValueDiction.Add(keyField.Name, keyValue);
           //分析字段类型
           foreach (System.Reflection.FieldInfo fieldInfo in fields)
           {
               if (fieldInfo == keyField)
                   continue;
               Type dataType = fieldInfo.FieldType;
               if (dataType.Equals(typeof(byte[])))
               {
                   blobFieldList.Add(fieldInfo);
                   continue;
               }
               object value = fieldInfo.GetValue(model);

               //为空 则不更新此字段；即认为空字段是不更新字段
               if (value == null)
                   continue;
               fieldValueDiction.Add(fieldInfo.Name, value);
           }
           //执行SQL语句
           bool result = dbHelper.updateByParams(tabName, fieldValueDiction);
           return result;
       }

        /// <summary>
       /// 插入模型记录
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dbHelper"></param>
        /// <param name="model"></param>
        /// <param name="keyFieldName"></param>
        /// <returns></returns>
       public static bool insertByModel<T>(IDBHelper dbHelper, T model)
       {
           Type type = typeof(T);
           //表名
           string tabName = type.Name;
           //属性信息字段
           System.Reflection.FieldInfo[] fields = type.GetFields();

           //大字段列
           List<FieldInfo> blobFieldList = new List<FieldInfo>();
           System.Reflection.FieldInfo keyField = null;
           //if (!string.IsNullOrEmpty(keyFieldName))
           //    keyField = type.GetField(keyFieldName);
           if (keyField == null)
               keyField = fields[0];
           string keyValue = keyField.GetValue(model).ToString();
           Dictionary<string, object> fieldValueDiction = new Dictionary<string, object>();
           fieldValueDiction.Add(keyField.Name, keyValue);
           //分析字段类型
           foreach (System.Reflection.FieldInfo fieldInfo in fields)
           {
               if (fieldInfo == keyField)
                   continue;
               Type dataType = fieldInfo.FieldType;
               if (dataType.Equals(typeof(byte[])))
               {
                   blobFieldList.Add(fieldInfo);
                   continue;
               }
               object value = fieldInfo.GetValue(model);

               //为空 则不更新此字段；即认为空字段是不更新字段
               if (value == null)
                   continue;
               fieldValueDiction.Add(fieldInfo.Name, value);
           }
           //执行SQL语句
           bool result = dbHelper.insertByParams(tabName, fieldValueDiction);
           return result;
       }
        /// <summary>
        /// 更新大字段内容
        /// </summary>
        /// <param name="tabName"></param>
        /// <param name="blobField"></param>
        /// <param name="contentBuffer"></param>
       /// <param name="clause"></param>
        /// <returns></returns>
       public static bool updateBlobField(IDBBlob dbHelper, string tabName, string blobField, byte[] contentBuffer, string clause)
       {
           return dbHelper.writeBlobContent(tabName, blobField, contentBuffer, clause);
       }
    }
}