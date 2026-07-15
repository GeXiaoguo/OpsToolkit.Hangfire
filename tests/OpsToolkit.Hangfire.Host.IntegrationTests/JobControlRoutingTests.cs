using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpsToolkit.Hangfire.JobControl;
using Shouldly;
using Xunit;

namespace OpsToolkit.Hangfire.Host.IntegrationTests;

public class JobControlRoutingTests
{
    [Fact]
    public void MapJobControl_CustomApiBase_DerivesRecurringAndRunsRoutes_Test()
    {
        var builder = WebApplication.CreateBuilder();
        using var app = builder.Build();

        app.MapJobControl(
            viewPolicy: "view",
            managePolicy: "manage",
            apiBase: "/operations/hangfire-api/");

        var routes = ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(source => source.Endpoints)
            .OfType<RouteEndpoint>()
            .Select(endpoint => endpoint.RoutePattern.RawText)
            .ToArray();

        routes.ShouldContain("/operations/hangfire-api/recurring/");
        routes.ShouldContain("/operations/hangfire-api/recurring/{jobId}/disable");
        routes.ShouldContain("/operations/hangfire-api/runs/stats");
        routes.ShouldContain("/operations/hangfire-api/runs/{id}/cancel");
        routes.ShouldNotContain(route => route != null && route.Contains("//", StringComparison.Ordinal));
    }

    [Fact]
    public void MapJobControl_EmptyApiBase_IsRejected_Test()
    {
        var builder = WebApplication.CreateBuilder();
        using var app = builder.Build();

        Should.Throw<ArgumentException>(() => app.MapJobControl("view", "manage", apiBase: " "));
    }

    [Fact]
    public async Task MapJobControl_CustomApiBase_IsInjectedIntoBothUis_Test()
    {
        using var host = await new HostBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.ConfigureServices(services =>
                {
                    services.AddRouting();
                    services.AddAuthorization(options =>
                    {
                        options.AddPolicy("view", policy => policy.RequireAssertion(_ => true));
                        options.AddPolicy("manage", policy => policy.RequireAssertion(_ => true));
                    });
                });
                web.Configure(app => app
                    .UseRouting()
                    .UseAuthorization()
                    .UseEndpoints(endpoints => endpoints.MapJobControl(
                        "view",
                        "manage",
                        apiBase: "/operations/hangfire-api/")));
            })
            .StartAsync();

        var client = host.GetTestClient();
        var recurringHtml = await client.GetStringAsync(JobControlEndpoints.DefaultUiPath);
        var runsHtml = await client.GetStringAsync(RunEndpoints.DefaultUiPath);

        recurringHtml.ShouldContain("var API_BASE = \"/operations/hangfire-api/recurring\";");
        runsHtml.ShouldContain("var API_BASE = \"/operations/hangfire-api/runs\";");
    }
}
