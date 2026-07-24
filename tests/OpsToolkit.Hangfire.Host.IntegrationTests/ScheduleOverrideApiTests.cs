using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Shouldly;
using Xunit;

namespace OpsToolkit.Hangfire.Host.IntegrationTests;

// Drives the schedule-override endpoints against the real sample host, whose demo jobs are declared
// through a RecurringJobRegistrar shared via JobControlOptions.Registrar (see Program.cs) — except
// "dashboard-pilot-job", registered with a plain AddOrUpdate precisely to exercise Declared=false.
// [Collection("Sample host")]: shares the sequential collection with the other host-booting classes.
[Collection("Sample host")]
public class ScheduleOverrideApiTests : IClassFixture<WebApplicationFactory<Program>>
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private readonly HttpClient _client;

    public ScheduleOverrideApiTests(WebApplicationFactory<Program> factory)
    {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task List_CarriesDeclaredFlagAndCodeDefault_Test()
    {
        var jobs = await getJobs();

        var declared = jobs.Single(j => j.Id == "heartbeat-every-minute");
        declared.Declared.ShouldBe(true);
        declared.CronDefault.ShouldBe("* * * * *");

        var pilot = jobs.Single(j => j.Id == "dashboard-pilot-job");
        pilot.Declared.ShouldBe(false);
        pilot.CronDefault.ShouldBeNull();
    }

    [Fact]
    public async Task CronOverride_AppliesImmediately_PersistsAndResets_Test()
    {
        const string jobId = "nightly-report";
        const string codeDefault = "0 0 * * *"; // Cron.Daily()

        try
        {
            // A crashed earlier run may have left an override behind — start from a known state so
            // the OldCron assertion below is deterministic. 404 (nothing to reset) is fine.
            await _client.PostAsJsonAsync($"/hangfire/api/recurring/{jobId}/cron/reset", new { reason = "test setup" });

            var reason = $"override test {Guid.NewGuid()}";
            (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{jobId}/cron", new { cron = "0 4 * * *", reason }))
                .EnsureSuccessStatusCode();

            var job = await getJob(jobId);
            job.Cron.ShouldBe("0 4 * * *");           // projected immediately — effective schedule
            job.CronDefault.ShouldBe(codeDefault);     // code default still reported alongside
            job.Override.ShouldNotBeNull();
            job.Override!.Cron.ShouldBe("0 4 * * *");
            job.Override.Reason.ShouldBe(reason);
            string.IsNullOrEmpty(job.Override.InvalidatedAt).ShouldBeTrue();

            var overrideEntry = (await getAudit(jobId, limit: 1)).Single();
            overrideEntry.Action.ShouldBe("cron-override");
            overrideEntry.Outcome.ShouldBe("ok");
            overrideEntry.Detail!["OldCron"].ShouldBe(codeDefault);
            overrideEntry.Detail["NewCron"].ShouldBe("0 4 * * *");

            (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{jobId}/cron/reset", new { reason = "back to normal" }))
                .EnsureSuccessStatusCode();

            job = await getJob(jobId);
            job.Cron.ShouldBe(codeDefault);
            job.Override.ShouldBeNull();

            var resetEntry = (await getAudit(jobId, limit: 1)).Single();
            resetEntry.Action.ShouldBe("cron-reset");
            resetEntry.Outcome.ShouldBe("ok");
            resetEntry.Detail!["RestoredCron"].ShouldBe(codeDefault);
        }
        finally
        {
            // Idempotent cleanup — a mid-test failure must not leave the shared demo job overridden.
            await _client.PostAsJsonAsync($"/hangfire/api/recurring/{jobId}/cron/reset", new { reason = "test cleanup" });
        }
    }

    [Fact]
    public async Task CronOverride_InvalidCron_IsRejectedByHangfiresOwnParser_Test()
    {
        await _client.PostAsJsonAsync("/hangfire/api/recurring/nightly-report/cron/reset", new { reason = "test setup" });

        var response = await _client.PostAsJsonAsync(
            "/hangfire/api/recurring/nightly-report/cron", new { cron = "95 99 * * *", reason = "test" });

        response.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
        // Nothing was persisted: the row is only written after a successful projection.
        (await getJob("nightly-report")).Override.ShouldBeNull();
    }

    [Fact]
    public async Task CronOverride_MissingReason_ReturnsBadRequest_Test()
    {
        var response = await _client.PostAsJsonAsync(
            "/hangfire/api/recurring/nightly-report/cron", new { cron = "0 4 * * *", reason = (string?)null });
        response.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task CronOverride_UndeclaredJob_ReturnsNotFound_Test()
    {
        // Exists in storage, but not declared through the registrar — there is no code definition to
        // rebuild the Job from, so it cannot carry an override.
        var response = await _client.PostAsJsonAsync(
            "/hangfire/api/recurring/dashboard-pilot-job/cron", new { cron = "0 4 * * *", reason = "test" });
        response.StatusCode.ShouldBe(HttpStatusCode.NotFound);

        var entry = (await getAudit("dashboard-pilot-job", limit: 1)).Single();
        entry.Action.ShouldBe("cron-override");
        entry.Outcome.ShouldBe("not-found");
    }

    [Fact]
    public async Task CronReset_WithoutOverride_ReturnsNotFound_Test()
    {
        var response = await _client.PostAsJsonAsync(
            "/hangfire/api/recurring/flaky-every-2-minutes/cron/reset", new { reason = "test" });
        response.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Reconcile_ReturnsSummary_ReportsUndeclared_AndIsAudited_Test()
    {
        var response = await _client.PostAsync("/hangfire/api/recurring/reconcile", content: null);
        response.EnsureSuccessStatusCode();

        var summary = (await response.Content.ReadFromJsonAsync<ReconcileSummaryDto>(JsonOptions))!;
        summary.Projected.ShouldBeGreaterThanOrEqualTo(5);
        summary.UndeclaredJobIds.ShouldContain("dashboard-pilot-job");
        summary.RemovedUndeclaredJobIds.ShouldBeEmpty(); // the endpoint never removes — startup policy only

        var entry = (await getAudit(limit: 5)).First(e => e.Action == "reconcile");
        entry.JobId.ShouldBe("*");
        entry.Actor.ShouldNotBeNullOrEmpty();
    }

    private async Task<List<RecurringJobSummaryDto>> getJobs()
    {
        var response = await _client.GetAsync("/hangfire/api/recurring");
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<List<RecurringJobSummaryDto>>(JsonOptions))!;
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
        return (await response.Content.ReadFromJsonAsync<List<AuditEntryDto>>(JsonOptions))!;
    }

    // Wire-shape mirrors, per the JobControlApiTests convention — this test depends only on what an
    // external consumer sees.
    private sealed record RecurringJobSummaryDto(
        string Id,
        string Cron,
        string? CronDefault,
        bool? Declared,
        OverrideRowDto? Override);

    private sealed record OverrideRowDto(
        string? Cron, string? ArgsJson, string UpdatedBy, string UpdatedAt, string? Reason, string? InvalidatedAt, string? InvalidatedReason);

    private sealed record ReconcileSummaryDto(
        int Projected,
        List<string> OverriddenJobIds,
        List<string> InvalidatedJobIds,
        List<string> RevalidatedJobIds,
        List<string> RemovedUndeclaredJobIds,
        List<string> UndeclaredJobIds,
        List<string> Errors);

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
