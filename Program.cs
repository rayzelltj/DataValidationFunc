using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;

var builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();

builder.Services
    .AddApplicationInsightsTelemetryWorkerService()
    .ConfigureFunctionsApplicationInsights();

builder.Services.AddSingleton<RulesProvider>(sp =>
{
    var storageConn = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
    var rulesContainer = Environment.GetEnvironmentVariable("RulesContainer") ?? "rules";
    var rulesBlob = Environment.GetEnvironmentVariable("RulesBlobName") ?? "validation-rules.json";
    var logger = sp.GetRequiredService<ILogger<RulesProvider>>();
    return new RulesProvider(storageConn, rulesContainer, rulesBlob, logger);
});

builder.Build().Run();
