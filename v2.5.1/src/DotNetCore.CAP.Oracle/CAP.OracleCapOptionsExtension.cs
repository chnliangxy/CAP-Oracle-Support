// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using DotNetCore.CAP.Oracle;
using DotNetCore.CAP.Processor;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using System;

// ReSharper disable once CheckNamespace
namespace DotNetCore.CAP
{
    internal class OracleCapOptionsExtension : ICapOptionsExtension
    {
        private readonly Action<OracleOptions> _configure;

        public OracleCapOptionsExtension(Action<OracleOptions> configure)
        {
            _configure = configure;
        }

        public void AddServices(IServiceCollection services)
        {
            services.AddSingleton<CapStorageMarkerService>();
            services.AddSingleton<IStorage, OracleStorage>();
            services.AddSingleton<IStorageConnection, OracleStorageConnection>();

            services.AddScoped<ICapPublisher, OraclePublisher>();
            services.AddScoped<ICallbackPublisher, OraclePublisher>();

            services.AddTransient<ICollectProcessor, OracleCollectProcessor>();
            services.AddTransient<CapTransactionBase, OracleCapTransaction>();

            AddSingletionOracleOptions(services);
        }

        private void AddSingletionOracleOptions(IServiceCollection services)
        {
            var OracleOptions = new OracleOptions();

            _configure(OracleOptions);

            if (OracleOptions.DbContextType != null)
            {
                services.AddSingleton(x =>
                {
                    using (var scope = x.CreateScope())
                    {
                        var provider = scope.ServiceProvider;
                        var dbContext = (DbContext)provider.GetService(OracleOptions.DbContextType);
                        OracleOptions.ConnectionString = dbContext.Database.GetDbConnection().ConnectionString;
                        return OracleOptions;
                    }
                });
            }
            else
            {
                services.AddSingleton(OracleOptions);
            }
        }
    }
}