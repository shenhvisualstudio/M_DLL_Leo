using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;
using System.Xml;
using MySql.Data;

using Newtonsoft.Json;
using System.Data.OracleClient;
using System.Data.SqlClient;
using MySql.Data.MySqlClient;
using System.Data.OleDb;

namespace Leo
{
    namespace MySql
    {
        public class DataAccess
        {
            public string connectionString;
            public DataAccess(string TNS)
            {
                connectionString = TNS;
            }
            public DataTable ExecuteTable(string sql)
            {
                DataTable datatable = new DataTable();
                MySqlConnection myConnection = new MySqlConnection(connectionString);
                MySqlCommand myORACCommand = myConnection.CreateCommand();
                myORACCommand.CommandText = sql;
                myConnection.Open();
                MySqlDataReader dataReader = myORACCommand.ExecuteReader();
                try
                {    ///动态添加表的数据列  
                    for (int i = 0; i < dataReader.FieldCount; i++)
                    {
                        DataColumn myDataColumn = new DataColumn();
                        myDataColumn.DataType = dataReader.GetFieldType(i);
                        myDataColumn.ColumnName = dataReader.GetName(i);
                        datatable.Columns.Add(myDataColumn);
                    }

                    ///添加表的数据  
                    while (dataReader.Read())
                    {
                        DataRow myDataRow = datatable.NewRow();
                        for (int i = 0; i < dataReader.FieldCount; i++)
                        {
                            myDataRow[i] = dataReader[i];
                        }

                        datatable.Rows.Add(myDataRow);
                        myDataRow = null;
                    }
                    ///关闭数据读取器  
                    dataReader.Close();
                    myConnection.Close();
                    datatable.TableName = "table";
                    return datatable;
                }
                catch (Exception ex)
                {
                    ///抛出类型转换错误  
                    //SystemError.CreateErrorLog(ex.Message);  
                    throw new Exception(ex.Message, ex);
                }
            }
        }
    }

    namespace Oracle
    {    
        public class DataAccess
        {
            private class Executor
            {
                public bool IsExecutoring { get; set; }
                public OracleConnection Connection { get; set; }
                public OracleTransaction Transaction { get; set; }
            }
       
            public DataAccess(string TNS)
            {
                 connectionString = TNS;
            }

            /// <summary>
            /// 开始事务
            /// </summary>
            public void BeginTransaction()
            {
                if (executor != null && executor.IsExecutoring)
                {
                    executor.Transaction.Rollback();
                    executor.Connection.Close();
                    executor.IsExecutoring = false;
                }

                executor = new Executor();
                OracleConnection connection = new OracleConnection(connectionString);
                connection.Open();
                executor.Connection = connection;
                executor.Transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
                executor.IsExecutoring = true;
            }

            /// <summary>
            /// 提交
            /// </summary>
            public void CommitTransaction()
            {
                if (executor != null && executor.IsExecutoring)
                {
                    executor.Transaction.Commit();
                    executor.IsExecutoring = false;
                    executor.Connection.Close();
                }
            }

            /// <summary>
            /// 回滚
            /// </summary>
            public void RollbackTransaction()
            {
                if (executor != null && executor.IsExecutoring)
                {
                    executor.Transaction.Rollback();
                    executor.IsExecutoring = false;
                    executor.Connection.Close();
                }
            }

            /// <summary>
            /// 连接地址
            /// </summary>
            public string connectionString;
            /// <summary>
            /// 事务对象
            /// </summary>
            private Executor executor;

            #region 执行SQL，返回Void
            /// <summary
            /// 执行SQL，返回Void
            /// </summary>
            /// <param name="sql">sql语句</param>
            /// <returns></returns>
            public void ExecuteNonQuery(string sql)
            {
                OracleConnection myConnection = null;
                OracleCommand myORACCommand = null;

                if (executor != null && executor.IsExecutoring)
                {
                    myConnection = executor.Connection;
                    myORACCommand = myConnection.CreateCommand();
                    myORACCommand.Transaction = executor.Transaction;
                }
                else
                {
                    myConnection = new OracleConnection(connectionString);
                    myConnection.Open();
                    myORACCommand = myConnection.CreateCommand();
                }

                myORACCommand.CommandText = sql;
                myORACCommand.ExecuteNonQuery();                           
                myORACCommand.Cancel();

                if (executor == null || !executor.IsExecutoring) {
                    myConnection.Close();
                }                   
            }
            #endregion

            #region 执行SQL，返回DataTable
            /// <summary>
            /// 执行SQL，返回DataTable
            /// </summary>
            /// <param name="sql">sql语句</param>
            /// <returns></returns>
            public DataTable ExecuteTable(string sql)
            {

                OracleConnection myConnection = null;
                OracleCommand myORACCommand = null;
                DataTable datatable = null;

                try
                {
                    if (executor != null && executor.IsExecutoring)
                    {
                        myConnection = executor.Connection;
                        myORACCommand = myConnection.CreateCommand();
                        myORACCommand.Transaction = executor.Transaction;
                    }
                    else
                    {
                        myConnection = new OracleConnection(connectionString);
                        myConnection.Open();
                        myORACCommand = myConnection.CreateCommand();
                    }

                    datatable = new DataTable();
                    myORACCommand.CommandText = sql;

                    DataSet ds = new DataSet();
                    OracleDataAdapter da = new OracleDataAdapter(myORACCommand);
                    da.Fill(ds);
                    if (ds.Tables.Count != 0)
                    {
                        datatable = ds.Tables[0];
                    }

                    if (executor == null || !executor.IsExecutoring)
                    {
                        myORACCommand.Cancel();
                        myConnection.Close();
                    }

                    datatable.TableName = "table";
                    return datatable;
                }
                catch (Exception ex)
                {
                    ///抛出类型转换错误  
                    //SystemError.CreateErrorLog(ex.Message); 
                    myORACCommand.Cancel();
                    myConnection.Close();
                    throw new Exception(ex.Message, ex);
                }
            }
            #endregion


            #region 执行SQL，返回DataTable(含区域)
            /// <summary>
            /// 执行SQL，返回DataTable
            /// </summary>
            /// <param name="sql">sql语句</param>
            /// <param name="startRow">起始行</param>
            /// <param name="endRow">终止行</param>
            /// <returns></returns>
            public DataTable ExecuteTable(string sql, int startRow, int endRow)
            {
                OracleConnection myConnection = null;
                OracleCommand myORACCommand = null;

                try
                {
                    if (executor != null && executor.IsExecutoring)
                    {
                        myConnection = executor.Connection;
                        myORACCommand = myConnection.CreateCommand();
                        myORACCommand.Transaction = executor.Transaction;
                    }
                    else
                    {
                        myConnection = new OracleConnection(connectionString);
                        myConnection.Open();
                        myORACCommand = myConnection.CreateCommand();
                    }

                    myORACCommand.CommandText = sql;

                    OracleDataReader dataReader = myORACCommand.ExecuteReader();
                    DataTable datatable = new DataTable();
                    //动态添加表的数据列  
                    for (int i = 0; i < dataReader.FieldCount; i++)
                    {
                        DataColumn myDataColumn = new DataColumn();
                        myDataColumn.DataType = dataReader.GetFieldType(i);
                        myDataColumn.ColumnName = dataReader.GetName(i);
                        datatable.Columns.Add(myDataColumn);
                    }

                    int rowIndex = 0;
                    ///添加表的数据  
                    while (dataReader.Read())
                    {
                        rowIndex++;
                        if (rowIndex < startRow || rowIndex > endRow)
                        {
                            continue;
                        }
                        DataRow myDataRow = datatable.NewRow();
                        for (int i = 0; i < dataReader.FieldCount; i++)
                        {
                            myDataRow[i] = dataReader[i];
                        }

                        datatable.Rows.Add(myDataRow);
                        myDataRow = null;
                    }
                    ///关闭数据读取器  
                    dataReader.Close();

                    if (executor == null || !executor.IsExecutoring)
                    {
                        myORACCommand.Cancel();
                        myConnection.Close();
                    }
                    datatable.TableName = "table";
                    return datatable;
                }
                catch (Exception ex)
                {
                    ///抛出类型转换错误  
                    //SystemError.CreateErrorLog(ex.Message); 
                    myORACCommand.Cancel();
                    myConnection.Close();
                    throw new Exception(ex.Message, ex);
                }
            }
            #endregion


            //public DataTable ExecuteTable(string sql)
            //{
            //    DataTable datatable = new DataTable();  
            //    OracleConnection myConnection = new OracleConnection(connectionString);
            //    OracleCommand myORACCommand = myConnection.CreateCommand();
            //    myORACCommand.CommandText = sql;
            //    myConnection.Open();
            //    OracleDataReader dataReader = myORACCommand.ExecuteReader();
            //    try
            //    {    ///动态添加表的数据列  
            //        for (int i = 0; i < dataReader.FieldCount; i++)
            //        {
            //            DataColumn myDataColumn = new DataColumn();
            //            myDataColumn.DataType = dataReader.GetFieldType(i);
            //            myDataColumn.ColumnName = dataReader.GetName(i);
            //            datatable.Columns.Add(myDataColumn);
            //        }

            //        ///添加表的数据  
            //        while (dataReader.Read())
            //        {
            //            DataRow myDataRow = datatable.NewRow();
            //            for (int i = 0; i < dataReader.FieldCount; i++)
            //            {
            //                 myDataRow[i] = dataReader[i];
            //            }

            //            datatable.Rows.Add(myDataRow);
            //            myDataRow = null;
            //        }
            //        ///关闭数据读取器  
            //        dataReader.Close();
            //        myConnection.Close();
            //        datatable.TableName = "table";
            //        return datatable;
            //    }
            //    catch (Exception ex)
            //    {
            //        ///抛出类型转换错误  
            //        //SystemError.CreateErrorLog(ex.Message);  
            //        throw new Exception(ex.Message, ex);
            //    }                
            //}

            #region 执行存储过程函数，返回DataTable
            /// <summary>
            /// 执行存储过程函数，返回DataTable
            /// </summary>
            /// <param name="produceName">存储过程函数名</param>
            /// <param name="oracleParameters">参数</param>
            /// <returns></returns>
            public DataTable ExecuteStoredProcedure(string produceName, OracleParameter[] oracleParameters)
            {
                OracleConnection myConnection = null;       
                OracleCommand myORACCommand = null;
                DataTable datatable = null;

                try
                {
                    datatable = new DataTable();
                    myConnection = new OracleConnection(connectionString);
                    myConnection.Open();
                    myORACCommand = myConnection.CreateCommand();
                    myORACCommand.CommandText = produceName;
                    myORACCommand.CommandType = CommandType.StoredProcedure;
                    myORACCommand.Parameters.AddRange(oracleParameters);
                    OracleParameter p = new OracleParameter("outvar", OracleType.Cursor);
                    p.Direction = System.Data.ParameterDirection.Output;
                    myORACCommand.Parameters.Add(p);

                    DataSet ds = new DataSet();
                    OracleDataAdapter da = new OracleDataAdapter(myORACCommand);
                    da.Fill(ds);
                    if (ds.Tables.Count != 0)
                    {
                        datatable = ds.Tables[0];
                    }

                    myORACCommand.Cancel();
                    myConnection.Close();
                    datatable.TableName = "table";
                    return datatable;           
                }
                catch (Exception ex)
                {
                    ///抛出类型转换错误  
                    //SystemError.CreateErrorLog(ex.Message);  
                    myORACCommand.Cancel();
                    myConnection.Close();
                    throw new Exception(ex.Message, ex);
                }
            }
            #endregion
        }
    }

    namespace SqlServer
    {
        public class DataAccess
        {
            public string connectionString;
            public DataAccess(string TNS)
            {
                connectionString = TNS;
            }

            public DataTable ExecuteTable(string sql)
            {
                SqlConnection myConnection = null;
                SqlCommand myORACCommand = null;
                DataTable datatable = null;

                try
                {
                    datatable = new DataTable();
                    myConnection = new SqlConnection(connectionString);
                    myConnection.Open();
                    myORACCommand = myConnection.CreateCommand();
                    myORACCommand.CommandText = sql;

                    DataSet ds = new DataSet();
                    SqlDataAdapter da = new SqlDataAdapter(myORACCommand);
                    da.Fill(ds);
                    if (ds.Tables.Count != 0)
                    {
                        datatable = ds.Tables[0];
                    }

                    myORACCommand.Cancel();
                    myConnection.Close();
                    datatable.TableName = "table";
                    return datatable; 
                }
                catch (Exception ex)
                {
                    ///抛出类型转换错误  
                    //SystemError.CreateErrorLog(ex.Message);  
                    myORACCommand.Cancel();
                    myConnection.Close();
                    throw new Exception(ex.Message, ex);
                }
            }


            /// <summary>
            /// 执行SQL，返回DataTable
            /// </summary>
            /// <param name="sql">sql语句</param>
            /// <param name="startRow">起始行</param>
            /// <param name="endRow">终止行</param>
            /// <returns></returns>
            public DataTable ExecuteTable(string sql, int startRow, int endRow)
            {

                SqlConnection myConnection = null;
                SqlCommand myORACCommand = null;

                try
                {
                    myConnection = new SqlConnection(connectionString);
                    myConnection.Open();
                    myORACCommand = myConnection.CreateCommand();
                    myORACCommand.CommandText = sql;

                    SqlDataReader dataReader = myORACCommand.ExecuteReader();
                    DataTable datatable = new DataTable();
                    //动态添加表的数据列  
                    for (int i = 0; i < dataReader.FieldCount; i++)
                    {
                        DataColumn myDataColumn = new DataColumn();
                        myDataColumn.DataType = dataReader.GetFieldType(i);
                        myDataColumn.ColumnName = dataReader.GetName(i);
                        datatable.Columns.Add(myDataColumn);
                    }

                    int rowIndex = 0;
                    ///添加表的数据  
                    while (dataReader.Read())
                    {
                        rowIndex++;
                        if (rowIndex < startRow || rowIndex > endRow)
                        {
                            continue;
                        }
                        DataRow myDataRow = datatable.NewRow();
                        for (int i = 0; i < dataReader.FieldCount; i++)
                        {
                            myDataRow[i] = dataReader[i];
                        }

                        datatable.Rows.Add(myDataRow);
                        myDataRow = null;
                    }
                    ///关闭数据读取器  
                    dataReader.Close();

                    myORACCommand.Cancel();
                    myConnection.Close();
                    datatable.TableName = "table";
                    return datatable;
                }
                catch (Exception ex)
                {
                    ///抛出类型转换错误  
                    //SystemError.CreateErrorLog(ex.Message);  
                    myORACCommand.Cancel();
                    myConnection.Close();
                    throw new Exception(ex.Message, ex);
                }
            }

            //public DataTable ExecuteTable(string sql)
            //{
            //    DataTable datatable = new DataTable();
            //    SqlConnection myConnection = new SqlConnection(connectionString);
            //    SqlCommand  myORACCommand = myConnection.CreateCommand();
            //    myORACCommand.CommandText = sql;
            //    myConnection.Open();
            //    SqlDataReader dataReader = myORACCommand.ExecuteReader();
            //    try
            //    {    ///动态添加表的数据列  
            //        for (int i = 0; i < dataReader.FieldCount; i++)
            //        {
            //            DataColumn myDataColumn = new DataColumn();
            //            myDataColumn.DataType = dataReader.GetFieldType(i);
            //            myDataColumn.ColumnName = dataReader.GetName(i);
            //            datatable.Columns.Add(myDataColumn);
            //        }

            //        ///添加表的数据  
            //        while (dataReader.Read())
            //        {
            //            DataRow myDataRow = datatable.NewRow();
            //            for (int i = 0; i < dataReader.FieldCount; i++)
            //            {
            //                myDataRow[i] = dataReader[i];
            //            }

            //            datatable.Rows.Add(myDataRow);
            //            myDataRow = null;
            //        }
            //        ///关闭数据读取器  
            //        dataReader.Close();
            //        myConnection.Close();
            //        datatable.TableName = "table";
            //        return datatable;
            //    }
            //    catch (Exception ex)
            //    {
            //        ///抛出类型转换错误  
            //        //SystemError.CreateErrorLog(ex.Message);  
            //        throw new Exception(ex.Message, ex);
            //    }
            //}
        }
    }

    namespace Access
    {
        public class DataAccess
        {
            public string connectionString;
            public DataAccess(string TNS)
            {
                connectionString = TNS;
            }
            public DataTable ExecuteTable(string sql)
            {
                DataTable datatable = new DataTable();
                OleDbConnection myConnection = new OleDbConnection(connectionString);
                OleDbCommand myORACCommand = myConnection.CreateCommand();
                myORACCommand.CommandText = sql;
                myConnection.Open();
                OleDbDataReader dataReader = myORACCommand.ExecuteReader();
                try
                {    ///动态添加表的数据列  
                    for (int i = 0; i < dataReader.FieldCount; i++)
                    {
                        DataColumn myDataColumn = new DataColumn();
                        myDataColumn.DataType = dataReader.GetFieldType(i);
                        myDataColumn.ColumnName = dataReader.GetName(i);
                        datatable.Columns.Add(myDataColumn);
                    }

                    ///添加表的数据  
                    while (dataReader.Read())
                    {
                        DataRow myDataRow = datatable.NewRow();
                        for (int i = 0; i < dataReader.FieldCount; i++)
                        {
                            myDataRow[i] = dataReader[i];
                        }

                        datatable.Rows.Add(myDataRow);
                        myDataRow = null;
                    }
                    ///关闭数据读取器  
                    dataReader.Close();
                    myConnection.Close();
                    datatable.TableName = "table";
                    return datatable;
                }
                catch (Exception ex)
                {
                    ///抛出类型转换错误  
                    //SystemError.CreateErrorLog(ex.Message);  
                    dataReader.Close();
                    myConnection.Close();
                    throw new Exception(ex.Message, ex);
                }
            }
        }
    }

    namespace Xml
    {
        public class ToXml
        {
            public string name;
            public string names;
            public ToXml(string ListName)
            {
                name = ListName;
                names = ListName+"s";
            }
            public string GetData(DataTable dt)
            {
                try{
                    
                    string xml = string.Format("<Table>",names);
                    string[] aaa = new string[dt.Columns.Count];
                    int i = 0;
                    foreach (DataColumn dc in dt.Columns)
                    {
                        aaa[i] = dc.ColumnName;
                        i++;
                    }
                    foreach (DataRow row in dt.Rows)
                    {
                        xml = xml + string.Format("<Row>", name);
                        i = 0;

                        for (int j = 0; j < dt.Columns.Count; j++)
                        {
                            xml = xml + string.Format("<{0}>", aaa[i]) + row[aaa[i]] + string.Format("</{0}>", aaa[i]);
                            i++;
                        }
                        xml = xml + string.Format("</Row>", name);
                    }
                    xml = xml + string.Format("</Table>", names);

                    return xml;
                }
            
                catch (Exception ex)
                {
                    ///抛出类型转换错误  
                    //SystemError.CreateErrorLog(ex.Message);  
                    throw new Exception(ex.Message, ex);
                }
            }
        }

        public class Message
            {
                public string message;
                public Message(string msg)
                {
                    message = msg;
                }
                public string TrueXml(string xml)
                {
                    int node = xml.IndexOf(">");
                    var name = xml.Substring(1, node-2);
                    xml = "<Root><Success>True</Success><Message>"+message+"</Message>"+xml+"</Root>";
                    return xml;
                }
                public string TrueXml()
                {
                    var xml = "<Root><Success>True</Success><Message>" + message + "</Message></Root>";
                    return xml;
                }

                public string FalseXml(string xml)
                {
                    int node = xml.IndexOf(">");
                    var name = xml.Substring(1, node - 2);
                    xml = "<Root><Success>False</Success><Message>" + message + "</Message>" + xml + "</Root>";
                    return xml;
                }
                public string FalseXml()
                {
                    var xml = "<Root><Success>False</Success><Message>" + message + "</Message></Root>";
                    return xml;
                }
            }
        
        public class Parser
        {
            public string Xml;
            public XmlDocument xml = new XmlDocument();
            public XmlNodeList nodelists = null;//xml

            public Parser(string XmlString)
            {
                Xml = XmlString;
                xml.LoadXml(XmlString);
                nodelists = xml.SelectSingleNode("/").ChildNodes;
            }
            public bool IsSuccess()
            {
                try{
                bool success = false;
                foreach (XmlNode list in nodelists)
                {

                    foreach (XmlNode grandfather in list.ChildNodes)
                    {
                        XmlElement fathers = (XmlElement)grandfather;
                        foreach (XmlNode father in fathers.ChildNodes)
                        {
                            switch (father.Value)
                            {
                                case "true": success = true; break;
                                case "True": success = true; break;
                                case "TRUE": success = true; break;
                                case "false": success = false; break;
                                case "False": success = false; break;
                                case "FALSE": success = false; break;                            
                            }
                        }

                    }
                }
                return success;
                }

                catch (Exception ex)
                {
                    ///抛出类型转换错误  
                    //SystemError.CreateErrorLog(ex.Message);  
                    throw new Exception(ex.Message, ex);
                }
            }
            public string GetMessage()
            {
                try{
                
                List<string> a = new List<string>();
                foreach (XmlNode list in nodelists)
                {

                    foreach (XmlNode grandfather in list.ChildNodes)
                    {
                        XmlElement fathers = (XmlElement)grandfather;
                        foreach (XmlNode father in fathers.ChildNodes)
                        {
                            a.Add(father.Value);
                        }

                    }
                }
                string[] b = a.ToArray();
                return b[1];
                }

                catch (Exception ex)
                {
                    ///抛出类型转换错误  
                    //SystemError.CreateErrorLog(ex.Message);  
                    throw new Exception(ex.Message, ex);
                }
            }
            public Array ToArray()
            {
                try
                {
                    List<Array> qwe = new List<Array>();
                    List<string> c = new List<string>();
                    foreach (XmlNode list in nodelists)
                    {
                        foreach (XmlNode grandfather in list.ChildNodes)
                        {
                            XmlElement fathers = (XmlElement)grandfather;
                            foreach (XmlNode father in fathers.ChildNodes)
                            {
                                //f.Add(father.Value);
                                foreach (XmlNode children in father.ChildNodes)
                                {
                                    foreach (XmlNode child in children.ChildNodes)
                                    {
                                            c.Add(child.Value);
                                    }
                                }
                                Array ea = c.ToArray();
                                if (ea.Length != 0)
                                {
                                    qwe.Add(ea);
                                }
                                c.Clear();
                            }
                        }
                    }
                    return qwe.ToArray();
                }

                catch (Exception ex)
                {
                    ///抛出类型转换错误  
                    //SystemError.CreateErrorLog(ex.Message);  
                    throw new Exception(ex.Message, ex);
                }
            }
        }
    }

    namespace Data
    {
        public class Table
        {
            public DataTable dt;
            public Table(DataTable DataTable)
            {
                dt = DataTable;
            }
            public Array ToArray()
            {
                try
                {
                    List<Array> qwe = new List<Array>();
                    string[] aaa = new string[dt.Columns.Count];
                    foreach (DataRow row in dt.Rows)
                    {
                        for (int j = 0; j < dt.Columns.Count; j++)
                        {
                            aaa[j] = Convert.ToString(row[j]);
                        }
                        qwe.Add(aaa);
                        aaa = new string[dt.Columns.Count];
                    }
                    return qwe.ToArray();
                }

                catch (Exception ex)
                {
                    ///抛出类型转换错误  
                    //SystemError.CreateErrorLog(ex.Message);  
                    throw new Exception(ex.Message, ex);
                }
            }
        }
    }

    
}
