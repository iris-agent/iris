// ---------------------------------------------------------------------------
// The agent loads packages and runs plugins.
// ---------------------------------------------------------------------------

using Iris.Configuration;
using Iris.Configuration;
using Iris.Core;
using Iris.Core.Plugins;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using YamlDotNet.Serialization;

// ---------------------------------------------------------------------------
// Load YAML configuration
// ---------------------------------------------------------------------------
var yamlPath = Path.Combine(AppContext.BaseDirectory, "appsettings.yaml");

if (!File.Exists(yamlPath))
{
    Console.Error.WriteLine("Iris could not start because the configuration file 'appsettings.yaml' is missing.");
    Console.Error.WriteLine($"  Expected location: {yamlPath}");
    Environment.ExitCode = 1;
    return;
}

Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Loading configuration from: {yamlPath}");

var yamlContent = await File.ReadAllTextAsync(yamlPath);

var yamlDeserializer = new DeserializerBuilder()
    .WithCaseInsensitivePropertyMatching()
    .IgnoreUnmatchedProperties()
    .Build();

// Deserialize for Serilog bootstrap (needs logging config before the host is built)
var irisOptions = yamlDeserializer.Deserialize<IrisOptions>(yamlContent) ?? new IrisOptions();

// Flatten the full YAML into IConfiguration key/value pairs so plugin assemblies
// can bind their own option types via IConfiguration without the core naming those types.
var yamlGraph = yamlDeserializer.Deserialize<object>(yamlContent);
var flatConfig = YamlConfigurationFlattener.Flatten(yamlGraph).ToList();

// ---------------------------------------------------------------------------
// Bootstrap Serilog early so all host events are captured
// ---------------------------------------------------------------------------
var rollingInterval = Enum.TryParse<RollingInterval>(irisOptions.Logging.RollingInterval, ignoreCase: true, out var ri)
    ? ri
    : RollingInterval.Day;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
    .WriteTo.File(
        irisOptions.Logging.FilePath,
        rollingInterval: rollingInterval,
        outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

try
{
    Log.Information("Iris starting up.");
    Log.Information("Configuration loaded from: {ConfigPath}", yamlPath);

    var host = Host.CreateDefaultBuilder(args)
        .UseWindowsService(opts => opts.ServiceName = "Iris")
        .UseSerilog()
        .ConfigureAppConfiguration(builder =>
        {
            builder.AddInMemoryCollection(flatConfig);
        })
        .ConfigureServices((ctx, services) =>
        {
            var config = ctx.Configuration;

            services.AddSingleton<IConfiguration>(config);

            // Bind core options from IConfiguration — no plugin type names needed here
            services.Configure<IrisOptions>(config);

            // Register plugin system infrastructure
            services.AddSingleton<IPluginRegistry, PluginRegistry>();
            services.AddSingleton<DynamicPluginLoader>();
            services.AddSingleton<UnifiedPluginFactory>();
            services.AddSingleton<IPluginFactory>(sp => sp.GetRequiredService<UnifiedPluginFactory>());

            // Bootstrap plugins before pipeline starts
            services.AddHostedService<PluginBootstrapService>();

            // Register the pipeline engine as the hosted service
            services.AddHostedService<PipelineEngine>();
        })
        .Build();

    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Iris terminated unexpectedly.");
}
finally
{
    await Log.CloseAndFlushAsync();
}
