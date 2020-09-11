using Dapper;
using DotNetCore.CAP.Infrastructure;
using DotNetCore.CAP.Models;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DotNetCore.CAP.Oracle
{
    public class OracleStorageConnection : IStorageConnection
    {
        private readonly CapOptions _capOptions;
        private readonly string _publishTableName;
        private readonly string _receivedTableName;

        public OracleStorageConnection(OracleOptions options, CapOptions capOptions)
        {
            _capOptions = capOptions;
            Options = options;
            _publishTableName = Options.GetPublishedTableName();
            _receivedTableName = Options.GetReceivedTableName();
        }

        public OracleOptions Options { get; }

        public IStorageTransaction CreateTransaction()
        {
            return new OracleStorageTransaction(this);
        }

        public async Task<CapPublishedMessage> GetPublishedMessageAsync(long id)
        {
            var sql = $@"SELECT * FROM {Options.GetUserName()}.{_publishTableName} WHERE Id={id}";

            using (var connection = new OracleConnection(Options.ConnectionString))
            {
                return await connection.QueryFirstOrDefaultAsync<CapPublishedMessage>(sql);
            }
        }

        public async Task<IEnumerable<CapPublishedMessage>> GetPublishedMessagesOfNeedRetry()
        {
            var fourMinsAgo = DateTime.Now.AddMinutes(-4);
            var sql =
                $"SELECT * FROM {Options.GetUserName()}.{_publishTableName} WHERE Retries<{_capOptions.FailedRetryCount} AND Version='{_capOptions.Version}' AND Added<to_date('{fourMinsAgo}','yyyy-mm-dd,hh24:mi:ss') AND (StatusName = '{StatusName.Failed}' OR StatusName = '{StatusName.Scheduled}') and rownum < 200";

            using (var connection = new OracleConnection(Options.ConnectionString))
            {
                return await connection.QueryAsync<CapPublishedMessage>(sql);
            }
        }

        public void StoreReceivedMessage(CapReceivedMessage message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            var sql = $@"
INSERT INTO {Options.GetUserName()}.{_receivedTableName}(Id,Version,Name,""GROUP"",Content,Retries,Added,ExpiresAt,StatusName)
VALUES(:Id,'{_capOptions.Version}',:Name,'{message.Group}',:Content,:Retries,:Added,:ExpiresAt,:StatusName)";

            using (var connection = new OracleConnection(Options.ConnectionString))
            {
                _ = connection.Execute(sql, new { message.Id, message.Name, message.Content, message.Retries, message.Added, message.ExpiresAt, message.StatusName });
            }
        }

        public async Task<CapReceivedMessage> GetReceivedMessageAsync(long id)
        {
            var sql = $@"SELECT * FROM {Options.GetUserName()}.{_receivedTableName} WHERE Id={id}";
            using (var connection = new OracleConnection(Options.ConnectionString))
            {
                return await connection.QueryFirstOrDefaultAsync<CapReceivedMessage>(sql);
            }
        }

        public async Task<IEnumerable<CapReceivedMessage>> GetReceivedMessagesOfNeedRetry()
        {
            var fourMinsAgo = DateTime.Now.AddMinutes(-4);
            var sql =
                $"SELECT * FROM {Options.GetUserName()}.{_receivedTableName} WHERE Retries<{_capOptions.FailedRetryCount} AND Version='{_capOptions.Version}' AND Added<to_date('{fourMinsAgo}','yyyy-mm-dd,hh24:mi:ss') AND (StatusName = '{StatusName.Failed}' OR StatusName = '{StatusName.Scheduled}') and rownum < 200";
            using (var connection = new OracleConnection(Options.ConnectionString))
            {
                return await connection.QueryAsync<CapReceivedMessage>(sql);
            }
        }

        public bool ChangePublishedState(long messageId, string state)
        {
            var sql =
                $"UPDATE {Options.GetUserName()}.{_publishTableName} SET Retries=Retries+1,ExpiresAt=NULL,StatusName = '{state}' WHERE Id={messageId}";

            using (var connection = new OracleConnection(Options.ConnectionString))
            {
                return connection.Execute(sql) > 0;
            }
        }

        public bool ChangeReceivedState(long messageId, string state)
        {
            var sql =
                $"UPDATE {Options.GetUserName()}.{_receivedTableName} SET Retries=Retries+1,ExpiresAt=NULL,StatusName = '{state}' WHERE Id={messageId}";

            using (var connection = new OracleConnection(Options.ConnectionString))
            {
                return connection.Execute(sql) > 0;
            }
        }
    }
}
