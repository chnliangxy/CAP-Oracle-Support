using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using DotNetCore.CAP.Dashboard;
using Microsoft.Extensions.Logging;
using Oracle.ManagedDataAccess.Client;

namespace DotNetCore.CAP.Oracle
{
    public class OracleStorage : IStorage
    {
        private readonly CapOptions _capOptions;
        private readonly IDbConnection _existingConnection = null;
        private readonly ILogger _logger;
        private readonly OracleOptions _options;

        public OracleStorage(ILogger<OracleStorage> logger,
            OracleOptions options,
            CapOptions capOptions)
        {
            _options = options;
            _capOptions = capOptions;
            _logger = logger;
        }

        public IStorageConnection GetConnection()
        {
            return new OracleStorageConnection(_options, _capOptions);
        }

        public IMonitoringApi GetMonitoringApi()
        {
            return new OracleMonitoringApi(this, _options);
        }

        public async Task InitializeAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            var sql = CreateDbTablesScript(_options.TableNamePrefix);
            using (var connection = new OracleConnection(_options.ConnectionString))
            {
                await connection.ExecuteAsync(sql);
            }

            _logger.LogDebug("Ensuring all create database tables script are applied.");
        }

        protected virtual string CreateDbTablesScript(string prefix)
        {
            var batchSql =
                 $@"
                begin
                        declare tableRecExists integer;
                        begin
                            select count(1) into tableRecExists from user_tables where table_name ='{_options.GetReceivedTableName()}';
                            if tableRecExists=0 then
                                begin
                                    execute immediate'
                                    CREATE TABLE {_options.GetUserName()}.{_options.GetReceivedTableName()} (
                                           Id number(23,0) NOT NULL,
                                           Version varchar2(20) DEFAULT NULL,
                                           Name varchar2(400) NOT NULL,
                                           ""GROUP"" varchar2(200) DEFAULT NULL,
                                           Content clob,
                                           Retries number(11,0) DEFAULT NULL,
                                           Added date NOT NULL,
                                           ExpiresAt date DEFAULT NULL,
                                           StatusName varchar2(50) NOT NULL
                                        )';
                                       execute immediate 'ALTER TABLE {_options.GetReceivedTableName()} ADD CONSTRAINT PK_CAP_Received PRIMARY KEY (Id)';
                                       execute immediate 'CREATE INDEX IX_CAP_Received_ExpiresAt ON {_options.GetReceivedTableName()} (ExpiresAt)';
                                end;
                            end if;
                        end;

                        declare tablePubExists integer;
                        begin
                            select count(*) into tablePubExists from user_tables where table_name ='{_options.GetPublishedTableName()}';
                            if tablePubExists=0 then
                                begin
                                    execute immediate'
                                    CREATE TABLE {_options.GetUserName()}.{_options.GetPublishedTableName()} (
                                         Id number(23,0) NOT NULL,
                                         Version varchar2(20) DEFAULT NULL,
                                         Name varchar2(200) NOT NULL,
                                         Content clob,
                                         Retries number(11,0) DEFAULT NULL,
                                         Added date NOT NULL,
                                         ExpiresAt date DEFAULT NULL,
                                         StatusName varchar2(50) NOT NULL
                                        )';
                                        execute immediate 'ALTER TABLE {_options.GetPublishedTableName()} ADD CONSTRAINT PK_CAP_Published PRIMARY KEY (Id)';
                                        execute immediate 'CREATE INDEX IX_CAP_Published_ExpiresAt ON {_options.GetPublishedTableName()} (ExpiresAt)';
                                end;
                            end if;
                        end;
                end;
            ";
            return batchSql;
        }

        internal T UseConnection<T>(Func<IDbConnection, T> func)
        {
            IDbConnection connection = null;

            try
            {
                connection = CreateAndOpenConnection();
                return func(connection);
            }
            finally
            {
                ReleaseConnection(connection);
            }
        }

        internal IDbConnection CreateAndOpenConnection()
        {
            var connection = _existingConnection ?? new OracleConnection(_options.ConnectionString);

            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }

            return connection;
        }

        internal bool IsExistingConnection(IDbConnection connection)
        {
            return connection != null && ReferenceEquals(connection, _existingConnection);
        }

        internal void ReleaseConnection(IDbConnection connection)
        {
            if (connection != null && !IsExistingConnection(connection))
            {
                connection.Dispose();
            }
        }
    }
}
