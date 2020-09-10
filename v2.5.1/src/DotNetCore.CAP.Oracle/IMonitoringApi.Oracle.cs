﻿// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Dapper;
using DotNetCore.CAP.Dashboard;
using DotNetCore.CAP.Dashboard.Monitoring;
using DotNetCore.CAP.Infrastructure;
using DotNetCore.CAP.Models;

namespace DotNetCore.CAP.Oracle
{
    internal class OracleMonitoringApi : IMonitoringApi
    {
        private readonly OracleStorage _storage;
        private readonly OracleOptions _options;

        public OracleMonitoringApi(IStorage storage, OracleOptions options)
        {
            _storage = storage as OracleStorage ?? throw new ArgumentNullException(nameof(storage));
            _options = options;
        }

        public StatisticsDto GetStatistics()
        {
            var sql = string.Format($@"
select count(Id) from {_options.GetUserName()}.{_options.GetPublishedTableName()} where StatusName = 'Succeeded';
select count(Id) from {_options.GetUserName()}.{_options.GetReceivedTableName()} where StatusName = 'Succeeded';
select count(Id) from {_options.GetUserName()}.{_options.GetPublishedTableName()} where StatusName = 'Failed';
select count(Id) from {_options.GetUserName()}.{_options.GetReceivedTableName()} where StatusName = 'Failed';");

            var statistics = UseConnection(connection =>
            {
                var stats = new StatisticsDto();
                using (var multi = connection.QueryMultiple(sql))
                {
                    stats.PublishedSucceeded = multi.ReadSingle<int>();
                    stats.ReceivedSucceeded = multi.ReadSingle<int>();

                    stats.PublishedFailed = multi.ReadSingle<int>();
                    stats.ReceivedFailed = multi.ReadSingle<int>();
                }

                return stats;
            });
            return statistics;
        }

        public IDictionary<DateTime, int> HourlyFailedJobs(MessageType type)
        {
            var tableName = type == MessageType.Publish ? _options.GetPublishedTableName() : _options.GetReceivedTableName();
            return UseConnection(connection =>
                GetHourlyTimelineStats(connection, tableName, StatusName.Failed));
        }

        public IDictionary<DateTime, int> HourlySucceededJobs(MessageType type)
        {
            var tableName = type == MessageType.Publish ? _options.GetPublishedTableName() : _options.GetReceivedTableName();
            return UseConnection(connection =>
                GetHourlyTimelineStats(connection, tableName, StatusName.Succeeded));
        }

        public IList<MessageDto> Messages(MessageQueryDto queryDto)
        {
            var tableName = queryDto.MessageType == MessageType.Publish ? _options.GetPublishedTableName() : _options.GetReceivedTableName();
            var where = string.Empty;
            if (!string.IsNullOrEmpty(queryDto.StatusName))
            {
                where += " and StatusName=:StatusName";
            }

            if (!string.IsNullOrEmpty(queryDto.Name))
            {
                where += " and Name=:Name";
            }

            if (!string.IsNullOrEmpty(queryDto.Group))
            {
                where += " and \"GROUP\"=:Group";
            }

            if (!string.IsNullOrEmpty(queryDto.Content))
            {
                where += " and Content like '%:Content%'";
            }

            var sqlQuery =
                $@"
                 SELECT * FROM
                 (
		                SELECT t1.*, ROW_NUMBER() OVER(ORDER BY Added DESC)
                    AS irowid FROM(
                            SELECT * FROM {_options.GetUserName()}.{tableName}
                            WHERE 1=1 {where}
                        ) t1
                ) t2
                WHERE t2.irowid > :Offset AND t2.irowid <= :Offset+:Limit";

            return UseConnection(conn => conn.Query<MessageDto>(sqlQuery, new
            {
                queryDto.StatusName,
                queryDto.Group,
                queryDto.Name,
                queryDto.Content,
                Offset = queryDto.CurrentPage * queryDto.PageSize,
                Limit = queryDto.PageSize
            }).ToList());
        }

        public int PublishedFailedCount()
        {
            return UseConnection(conn => GetNumberOfMessage(conn, "published", StatusName.Failed));
        }

        public int PublishedSucceededCount()
        {
            return UseConnection(conn => GetNumberOfMessage(conn, "published", StatusName.Succeeded));
        }

        public int ReceivedFailedCount()
        {
            return UseConnection(conn => GetNumberOfMessage(conn, "received", StatusName.Failed));
        }

        public int ReceivedSucceededCount()
        {
            return UseConnection(conn => GetNumberOfMessage(conn, "received", StatusName.Succeeded));
        }

        private int GetNumberOfMessage(IDbConnection connection, string tableName, string statusName)
        {
            var sqlQuery = $"select count(Id) from {_options.GetUserName()}.{tableName} where StatusName = :state";

            var count = connection.ExecuteScalar<int>(sqlQuery, new { state = statusName });
            return count;
        }

        private T UseConnection<T>(Func<IDbConnection, T> action)
        {
            return _storage.UseConnection(action);
        }

        private Dictionary<DateTime, int> GetHourlyTimelineStats(IDbConnection connection, string tableName,
            string statusName)
        {
            var endDate = DateTime.Now;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keyMaps = dates.ToDictionary(x => x.ToString("yyyy-MM-dd-HH"), x => x);

            return GetTimelineStats(connection, tableName, statusName, keyMaps);
        }

        private Dictionary<DateTime, int> GetTimelineStats(
            IDbConnection connection,
            string tableName,
            string statusName,
            IDictionary<string, DateTime> keyMaps)
        {
            var sqlQuery =
            $@"
                SELECT aggr.*
                FROM (
                         SELECT TO_CHAR(Added,'yyyy-mm-dd-hh24') AS Key, COUNT(Id) Count
                         FROM {_options.GetUserName()}.{tableName}
                         WHERE StatusName = :P_StatusName
                         GROUP BY TO_CHAR(Added,'yyyy-mm-dd-hh24')
                     ) aggr
                WHERE Key >= :P_MinKey AND Key <= :P_MaxKey";

            var valuesMap = connection.Query<TimelineCounter>(
                    sqlQuery,
                    new { keys = keyMaps.Keys, statusName })
                .ToDictionary(x => x.Key, x => x.Count);

            foreach (var key in keyMaps.Keys)
            {
                if (!valuesMap.ContainsKey(key))
                {
                    valuesMap.Add(key, 0);
                }
            }

            var result = new Dictionary<DateTime, int>();
            for (var i = 0; i < keyMaps.Count; i++)
            {
                var value = valuesMap[keyMaps.ElementAt(i).Key];
                result.Add(keyMaps.ElementAt(i).Value, value);
            }

            return result;
        }
    }
}