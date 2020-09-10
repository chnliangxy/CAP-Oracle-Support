using System;
using System.Data;
using System.Threading.Tasks;
using Dapper;
using DotNetCore.CAP.Models;
using Oracle.ManagedDataAccess.Client;

namespace DotNetCore.CAP.Oracle
{
    public class OracleStorageTransaction : IStorageTransaction
    {
        private readonly IDbConnection _dbConnection;

        private readonly string _publishedTableName;
        private readonly string _receivedTableName;
        private readonly string _userName;

        public OracleStorageTransaction(OracleStorageConnection connection)
        {
            var options = connection.Options;
            _publishedTableName = options.GetPublishedTableName();
            _receivedTableName = options.GetReceivedTableName();
            _userName = options.GetUserName();
            _dbConnection = new OracleConnection(options.ConnectionString);
        }

        public void UpdateMessage(CapPublishedMessage message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            var sql =
                $"UPDATE {_userName}.{_publishedTableName} SET Retries = :Retries,Content= :Content,ExpiresAt = :ExpiresAt,StatusName=:StatusName WHERE Id=:Id";
            _dbConnection.Execute(sql, message);
        }

        public void UpdateMessage(CapReceivedMessage message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            var sql =
                $"UPDATE {_userName}.{_receivedTableName} SET Retries = :Retries,Content= :Content,ExpiresAt = :ExpiresAt,StatusName=:StatusName WHERE Id=:Id";
            _dbConnection.Execute(sql, message);
        }

        public Task CommitAsync()
        {
            _dbConnection.Close();
            _dbConnection.Dispose();
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _dbConnection.Dispose();
        }
    }
}
