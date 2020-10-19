using System;
using System.Configuration;
using System.Linq;

namespace Atomus.Database
{
    public class DatabaseAdapter : IDatabaseAdapter
    {
        string[] IDatabaseAdapter.DatabaseConnectionNames
        {
            get
            {
                var databaseNames = from allKeys in ConfigurationManager.AppSettings.AllKeys
                                    where allKeys.Contains("ConnectionString.")
                                    select allKeys.Substring(allKeys.IndexOf(".") + 1);

                return databaseNames.ToArray();
            }
        }

        IDatabase IDatabaseAdapter.CreateDatabase()
        {
            return ((IDatabaseAdapter)this).CreateDatabase("");
        }

        /// <summary>
        /// Database 생성
        /// ConnectionString MS-SQL : Data Source=127.0.0.1;Initial Catalog=DBNAME;Persist Security Info=True;User ID=sa;Password=pass
        /// ConnectionString Excel FIle : Provider=Microsoft.ACE.OLEDB.12.0;Data Source=E:\바탕화면\Data.xlsx;Extended Properties='Excel 12.0 XML;HDR=Yes;READONLY=FALSE'
        ///                               Provider=Microsoft.ACE.OLEDB.12.0;Data Source={0};Mode=ReadWrite|Share Deny None;Extended Properties='Excel 12.0; HDR={1};IMEX={2};READONLY=FALSE';Persist Security Info=False;
        /// ConnectionString Excel FIle : Provider=Microsoft.Jet.OLEDB.4.0;Data Source=E:\바탕화면\Data.xls;Extended Properties='Excel 8.0;HDR=Yes;READONLY=FALSE'
        ///                               Provider=Microsoft.Jet.OLEDB.4.0;Data Source={0};Mode=ReadWrite|Share Deny None;Extended Properties='Excel 8.0; HDR={1};IMEX={2};READONLY=FALSE';Persist Security Info=False;
        /// </summary>
        /// <param name="connectionName">connection 이름</param>
        /// <returns>생성된 데이터 베이스를 반환 합니다.</returns>
        IDatabase IDatabaseAdapter.CreateDatabase(string connectionName)
        {
            string commandTimeout;
            string databaseNamespace;

            try
            {
                if (connectionName != "")
                    connectionName = string.Format(".{0}", connectionName);

                databaseNamespace = this.GetAttribute(string.Format("Provider{0}", connectionName));

                if (databaseNamespace == null)
                    throw new AtomusException(string.Format("데이터 베이스 Provider{0}가 없습니다.", connectionName));

                if (ConfigurationManager.AppSettings[string.Format("ConnectionString{0}", connectionName)] == null)
                    throw new AtomusException(string.Format("연결 문자열 ConnectionString{0}가 없습니다.", connectionName));

                commandTimeout = this.GetAttribute("CommandTimeout");

                if (commandTimeout == null)
                    commandTimeout = "60000";

                return ((IDatabaseAdapter)this).CreateDatabase(databaseNamespace, ConfigurationManager.AppSettings[string.Format("ConnectionString{0}", connectionName)], commandTimeout.ToInt());
            }
            catch (AtomusException exception)
            {
                throw exception;
            }
            catch (Exception exception)
            {
                throw new AtomusException(exception);
            }
        }

        IDatabase IDatabaseAdapter.CreateDatabase(string providerNamespace, string connectionString, int commandTimeout)
        {
            IDatabase database;

            try
            {
                database = (IDatabase)Factory.CreateInstance(providerNamespace, false, true);
                //database = new Database.Influx();
                //database = (IDatabase)Factory.CreateInstance(@"E:\Work\Project\Atomus\Database\MySQL\bin\Debug\Atomus.Database.MySQL.dll", "Atomus.Database.MySQL", false, true);

                database.Connection.ConnectionString = connectionString;
                database.Command.CommandTimeout = commandTimeout;

                return database;
            }
            catch (AtomusException exception)
            {
                throw exception;
            }
            catch (Exception exception)
            {
                throw new AtomusException(exception);
            }
        }
    }
}