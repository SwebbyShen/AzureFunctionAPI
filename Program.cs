using billoneexactapi;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;



var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults(
    builder => builder.UseMiddleware<ExceptionHandlingMiddleware>()
    )
    //.ConfigureServices(config)
    .Build();

host.Run();

//var config = new ConfigurationBuilder()
//            .SetBasePath(Environment.CurrentDirectory)
//            .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
//            .AddEnvironmentVariables()
//            .Build();

// var ConfigureServices=new IServiceCollection services

//    services.AddLogging();
//    services.AddSingleton<IConfiguration>(provider =>
//    {
//        var configBuilder = new ConfigurationBuilder()
//            .SetBasePath(Environment.CurrentDirectory)
//            .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true);

//        return configBuilder.Build();
//    });
//    services.AddFunctionsWorkerDefaults();
//    services.AddHttpClient();




