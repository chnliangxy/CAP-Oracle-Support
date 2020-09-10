using System;
using System.Threading.Tasks;
using Dapper;
using DotNetCore.CAP.Processor;
using Microsoft.Extensions.Logging;
using Oracle.ManagedDataAccess.Client;

namespace DotNetCore.CAP.Oracle
{
    internal class OracleCollectProcessor : ICollectProcessor
    {
        private const int MaxBatch = 1000;
        private readonly TimeSpan _delay = TimeSpan.FromSeconds(1);
        private readonly ILogger _logger;
        private readonly OracleOptions _options;
        private readonly string _publishedTableName;
        private readonly string _receivedTableName;
        private readonly TimeSpan _waitingInterval = TimeSpan.FromMinutes(5);

        public OracleCollectProcessor(ILogger<OracleCollectProcessor> logger,
            OracleOptions OracleOptions)
        {
            _logger = logger;
            _options = OracleOptions;
            _publishedTableName = _options.GetPublishedTableName();
            _receivedTableName = _options.GetReceivedTableName();
        }

        public async Task ProcessAsync(ProcessingContext context)
        {
            var tables = new[]
            {
                _publishedTableName,
                _receivedTableName
            };

            foreach (var table in tables)
            {
                _logger.LogDebug($"Collecting expired data from table [{table}].");

                int removedCount;
                do
                {
                    using (var connection = new OracleConnection(_options.ConnectionString))
                    {
                        removedCount = await connection.ExecuteAsync(
                     $@"DELETE FROM {table} WHERE ExpiresAt < :now AND rownum < :count",
                     new { now = DateTime.Now, count = MaxBatch });
                    }

                    if (removedCount != 0)
                    {
                        await context.WaitAsync(_delay);
                        context.ThrowIfStopping();
                    }
                } while (removedCount != 0);
            }

            await context.WaitAsync(_waitingInterval);
        }
    }
}
