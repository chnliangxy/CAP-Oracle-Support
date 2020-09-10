
using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using DotNetCore.CAP.Abstractions;
using DotNetCore.CAP.Models;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Oracle.ManagedDataAccess.Client;

namespace DotNetCore.CAP.Oracle
{
    public class OraclePublisher : CapPublisherBase, ICallbackPublisher
    {
        private readonly OracleOptions _options;

        public OraclePublisher(IServiceProvider provider) : base(provider)
        {
            _options = provider.GetService<OracleOptions>();
        }

        public async Task PublishCallbackAsync(CapPublishedMessage message)
        {
            await PublishAsyncInternal(message);
        }

        protected override async Task ExecuteAsync(CapPublishedMessage message, ICapTransaction transaction,
            CancellationToken cancel = default(CancellationToken))
        {
            if (NotUseTransaction)
            {
                using (var connection = new OracleConnection(_options.ConnectionString))
                {
                    await connection.ExecuteAsync(PrepareSql(), message);
                    return;
                }
            }

            var dbTrans = transaction.DbTransaction as IDbTransaction;
            if (dbTrans == null && transaction.DbTransaction is IDbContextTransaction dbContextTrans)
            {
                dbTrans = dbContextTrans.GetDbTransaction();
            }

            var conn = dbTrans?.Connection;
            await conn.ExecuteAsync(PrepareSql(), message, dbTrans);
        }

        #region private methods

        private string PrepareSql()
        {
            return
            $"INSERT INTO {_options.GetUserName()}.{_options.GetPublishedTableName()}(Id,Version,Name,Content,Retries,Added,ExpiresAt,StatusName)" +
                      $" VALUES(:Id,'{_options.Version}',:Name,:Content,:Retries,:Added,:ExpiresAt,:StatusName)";
        }

        #endregion private methods
    }
}
