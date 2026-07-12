using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Shouldly;
using Xunit;

namespace Hangfire.OpsToolkit.Host.IntegrationTests;

// Drives the job-control HTTP API against the real sample host (real Postgres storage, real
// AddHangfireServer()) — the demo recurring jobs registered in Program.cs are the fixtures.
public class JobControlApiTests : IClassFixture<WebApplicationFactory<Program>>
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private readonly HttpClient _client;

    public JobControlApiTests(WebApplicationFactory<Program> factory)
    {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task ListRecurringJobs_IncludesSeededDemoJobs_Test()
    {
        var jobs = await getJobs();

        jobs.ShouldContain(j => j.Id == "heartbeat-every-minute");
        jobs.ShouldContain(j => j.Id == "nightly-report");
    }

    [Fact]
    public async Task DisableThenEnable_RoundTrips_Test()
    {
        const string jobId = "nightly-report";

        (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{jobId}/disable", new { reason = "integration test" }))
            .EnsureSuccessStatusCode();
        (await getJob(jobId)).DisableStatus?.Disabled.ShouldBeTrue();

        (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{jobId}/enable", new { reason = (string?)null }))
            .EnsureSuccessStatusCode();
        (await getJob(jobId)).DisableStatus?.Disabled.ShouldBeFalse();
    }

    [Fact]
    public async Task Disable_MissingReason_ReturnsBadRequest_Test()
    {
        var response = await _client.PostAsJsonAsync("/hangfire/api/recurring/nightly-report/disable", new { reason = (string?)null });
        response.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Disable_UnknownJob_ReturnsNotFound_Test()
    {
        var response = await _client.PostAsJsonAsync($"/hangfire/api/recurring/does-not-exist-{Guid.NewGuid()}/disable", new { reason = "test" });
        response.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Trigger_KnownJob_ReturnsOk_Test()
    {
        var response = await _client.PostAsync("/hangfire/api/recurring/heartbeat-every-minute/trigger", content: null);
        response.EnsureSuccessStatusCode();
    }

    [Fact]
    public async Task Disable_IsAudited_Atomically_Test()
    {
        const string jobId = "nightly-report";
        var reason = $"audit test {Guid.NewGuid()}";

        try
        {
            (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{jobId}/disable", new { reason }))
                .EnsureSuccessStatusCode();

            var entry = (await getAudit(jobId, limit: 1)).Single();
            entry.Actor.ShouldNotBeNullOrEmpty();
            entry.Action.ShouldBe("disable");
            entry.JobId.ShouldBe(jobId);
            entry.Reason.ShouldBe(reason);
            entry.Outcome.ShouldBe("ok");
        }
        finally
        {
            await _client.PostAsJsonAsync($"/hangfire/api/recurring/{jobId}/enable", new { reason = (string?)null });
        }
    }

    [Fact]
    public async Task Disable_UnknownJob_AuditedNotFound_Test()
    {
        var jobId = $"does-not-exist-{Guid.NewGuid()}";

        (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{jobId}/disable", new { reason = "test" }))
            .StatusCode.ShouldBe(HttpStatusCode.NotFound);

        var entry = (await getAudit(jobId, limit: 1)).Single();
        entry.Action.ShouldBe("disable");
        entry.Outcome.ShouldBe("not-found");
    }

    [Fact]
    public async Task Trigger_Audit_CarriesBackgroundJobId_Test()
    {
        const string jobId = "heartbeat-every-minute";

        (await _client.PostAsync($"/hangfire/api/recurring/{jobId}/trigger", content: null))
            .EnsureSuccessStatusCode();

        var entry = (await getAudit(jobId, limit: 1)).Single();
        entry.Action.ShouldBe("trigger");
        entry.Outcome.ShouldBe("ok");
        entry.Detail.ShouldNotBeNull();
        entry.Detail!.ShouldContainKey("BackgroundJobId");
        entry.Detail["BackgroundJobId"].ShouldNotBeNullOrEmpty();
    }

    [Fact]
    public async Task Delete_UnknownJob_ReturnsNotFound_Test()
    {
        var response = await _client.PostAsync($"/hangfire/api/recurring/does-not-exist-{Guid.NewGuid()}/delete", content: null);
        response.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Delete_Audit_SnapshotsDefinition_Test()
    {
        // Not referenced by any other test in this class — safe to delete permanently.
        const string jobId = "flaky-every-2-minutes";

        (await _client.PostAsync($"/hangfire/api/recurring/{jobId}/delete", content: null))
            .EnsureSuccessStatusCode();

        (await getJobs()).ShouldNotContain(j => j.Id == jobId);

        var entry = (await getAudit(jobId, limit: 1)).Single();
        entry.Action.ShouldBe("delete");
        entry.Outcome.ShouldBe("ok");
        entry.Detail.ShouldNotBeNull();
        entry.Detail!["Cron"].ShouldBe("*/2 * * * *");
    }

    private async Task<List<RecurringJobSummaryDto>> getJobs()
    {
        var response = await _client.GetAsync("/hangfire/api/recurring");
        response.EnsureSuccessStatusCode();
        var jobs = await response.Content.ReadFromJsonAsync<List<RecurringJobSummaryDto>>(JsonOptions);
        return jobs!;
    }

    private async Task<RecurringJobSummaryDto> getJob(string jobId)
        => (await getJobs()).Single(j => j.Id == jobId);

    private async Task<List<AuditEntryDto>> getAudit(string? jobId = null, int? limit = null)
    {
        var query = new List<string>();
        if (jobId is not null) query.Add($"jobId={Uri.EscapeDataString(jobId)}");
        if (limit is not null) query.Add($"limit={limit}");
        var queryString = query.Count > 0 ? "?" + string.Join("&", query) : "";

        var response = await _client.GetAsync($"/hangfire/api/recurring/audit{queryString}");
        response.EnsureSuccessStatusCode();
        var entries = await response.Content.ReadFromJsonAsync<List<AuditEntryDto>>(JsonOptions);
        return entries!;
    }

    // Local mirror of JobControlEndpoints.RecurringJobSummary — kept separate from the library's own
    // record so this test only depends on the wire shape, the same way an external API consumer would.
    private sealed record RecurringJobSummaryDto(
        string Id,
        string Cron,
        string? TimeZoneId,
        string? JobDisplayName,
        DateTime? NextExecution,
        DateTime? LastExecution,
        string? LastJobId,
        string? LastJobState,
        DateTime? CreatedAt,
        string? Error,
        DisableStatusDto? DisableStatus);

    private sealed record DisableStatusDto(bool Disabled, string? By, string? At, string? Reason);

    // Local mirror of JobControlEndpoints.AuditEntry — same reasoning as RecurringJobSummaryDto above.
    private sealed record AuditEntryDto(
        int V,
        DateTime At,
        string Actor,
        string Action,
        string JobId,
        string? Reason,
        string Outcome,
        Dictionary<string, string>? Detail);
}
