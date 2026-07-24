using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Hangfire;
using Hangfire.Storage;
using Microsoft.AspNetCore.Mvc.Testing;
using Shouldly;
using Xunit;

namespace OpsToolkit.Hangfire.Host.IntegrationTests;

// Drives the cancel-request → abort → acknowledge protocol (RunEndpoints' POST /{id}/cancel,
// CancellationOutcomeFilter, CancellationRequestStore) against the real sample host — real Postgres
// storage, a real AddHangfireServer() with CancellationCheckInterval tuned to 1s in Program.cs so these
// tests don't wait out the 5s default. CancellationTestJobs.HonoringLoop/IgnoringLoop (also in Program.cs)
// are the fixtures: one flows its token into awaited work (observes an abort), one never checks it (runs
// to completion regardless (the cancellation protocol's "completed anyway" case).
// [Collection("Sample host")]: see SampleHostCollection — every WebApplicationFactory<Program>-backed
// class in this project joins it to avoid a real-hosting-startup race between them.
[Collection("Sample host")]
public class CancellationApiTests : IClassFixture<WebApplicationFactory<Program>>
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    private static readonly TimeSpan PollTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(150);

    private readonly HttpClient _client;

    public CancellationApiTests(WebApplicationFactory<Program> factory)
    {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task Cancel_Processing_AbortsAndAcks_Test()
    {
        var jobId = BackgroundJob.Enqueue<CancellationTestJobs>(x => x.HonoringLoop(default));
        (await waitForState(jobId, "Processing")).ShouldBe("Processing");

        (await cancel(jobId, "integration test", "Processing")).EnsureSuccessStatusCode();

        (await waitForState(jobId, "Deleted")).ShouldBe("Deleted");

        var ack = await waitForAuditAction(jobId, "cancel-ack");
        ack.ShouldNotBeNull();
        ack!.Detail!["Result"].ShouldBe("aborted");
        long.Parse(ack.Detail["ElapsedMs"]).ShouldBeGreaterThanOrEqualTo(0);

        // Polled like the ack above rather than fetched once: the entry is committed before the cancel
        // endpoint even responds, yet a single unpolled read here has been observed (rarely, on a CI VM)
        // to come back without it while a poll moments later sees it — flake-hardening, not semantics.
        var cancelEntry = await waitForAuditAction(jobId, "cancel");
        cancelEntry.ShouldNotBeNull();
        cancelEntry!.Outcome.ShouldBe("ok");
        cancelEntry.Reason.ShouldBe("integration test");
    }

    [Fact]
    public async Task Cancel_TokenIgnoringJob_RecordsCompletedAnyway_Test()
    {
        var jobId = BackgroundJob.Enqueue<CancellationTestJobs>(x => x.IgnoringLoop());
        (await waitForState(jobId, "Processing")).ShouldBe("Processing");

        (await cancel(jobId, "integration test", "Processing")).EnsureSuccessStatusCode();

        // IgnoringLoop sleeps 4s with no token — it outlives the cancel and runs to completion.
        var ack = await waitForAuditAction(jobId, "cancel-ack");
        ack.ShouldNotBeNull();
        ack!.Detail!["Result"].ShouldBe("completed-anyway");

        // Mechanic #6: the late completion can't un-delete the job.
        getJobState(jobId).ShouldBe("Deleted");
    }

    [Fact]
    public async Task Cancel_RequiresReason_Test()
    {
        var jobId = BackgroundJob.Enqueue<CancellationTestJobs>(x => x.HonoringLoop(default));
        var response = await cancel(jobId, reason: null, expectedState: "Enqueued");
        response.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Cancel_WrongState409_Test()
    {
        var jobId = BackgroundJob.Enqueue<CancellationTestJobs>(x => x.HonoringLoop(default));

        // The job is Enqueued or already Processing by now — Scheduled is a state it can never be in.
        var response = await cancel(jobId, "integration test", "Scheduled");
        response.StatusCode.ShouldBe(HttpStatusCode.Conflict);

        var entry = (await getRunsAudit(jobId)).First(e => e.Action == "cancel");
        entry.Outcome.ShouldBe("wrong-state");
        entry.Detail!.ShouldContainKey("CurrentState");
    }

    [Fact]
    public async Task Cancel_Unknown404_Test()
    {
        var jobId = $"does-not-exist-{Guid.NewGuid()}";
        var response = await cancel(jobId, "integration test", "Processing");
        response.StatusCode.ShouldBe(HttpStatusCode.NotFound);

        var entry = (await getRunsAudit(jobId)).First(e => e.Action == "cancel");
        entry.Outcome.ShouldBe("not-found");
    }

    [Fact]
    public async Task Cancel_Enqueued_NeverRuns_Test()
    {
        // No server listens on this queue (Program.cs's AddHangfireServer only configures "default") —
        // the same "nothing is running it" guarantee a paused queue would give, with no pause API needed.
        var jobId = BackgroundJob.Enqueue<CancellationTestJobs>("unwatched-test-queue", x => x.HonoringLoop(default));
        getJobState(jobId).ShouldBe("Enqueued");

        (await cancel(jobId, "integration test", "Enqueued")).EnsureSuccessStatusCode();

        getJobState(jobId).ShouldBe("Deleted");

        // Queued cancels are complete at step 1 (mechanic #7) — no marker, nothing to acknowledge.
        await Task.Delay(TimeSpan.FromSeconds(2));
        (await getRunsAudit(jobId)).ShouldNotContain(e => e.Action == "cancel-ack");
    }

    [Fact]
    public async Task BuiltInDelete_RecordsAbortObserved_Test()
    {
        var jobId = BackgroundJob.Enqueue<CancellationTestJobs>(x => x.HonoringLoop(default));
        (await waitForState(jobId, "Processing")).ShouldBe("Processing");

        // Bypasses the governed /cancel endpoint entirely — exactly what the built-in dashboard's own
        // Delete action does to a Processing job.
        BackgroundJob.Delete(jobId);

        var entry = await waitForAuditAction(jobId, "abort-observed");
        entry.ShouldNotBeNull();
        entry!.Actor.ShouldBe("unknown");
    }

    [Fact]
    public async Task DeletedList_MarksCancelledRows_Test()
    {
        var cancelledJobId = BackgroundJob.Enqueue<CancellationTestJobs>(x => x.HonoringLoop(default));
        await waitForState(cancelledJobId, "Processing");
        (await cancel(cancelledJobId, "integration test", "Processing")).EnsureSuccessStatusCode();
        await waitForState(cancelledJobId, "Deleted");

        var plainDeletedJobId = BackgroundJob.Enqueue<CancellationTestJobs>(x => x.HonoringLoop(default));
        await waitForState(plainDeletedJobId, "Processing");
        BackgroundJob.Delete(plainDeletedJobId);
        await waitForState(plainDeletedJobId, "Deleted");

        var rows = await getDeletedList();

        var cancelledRow = rows.First(r => r.Id == cancelledJobId);
        cancelledRow.Cancelled.ShouldBeTrue();
        cancelledRow.CancelledBy.ShouldNotBeNullOrEmpty();

        var plainRow = rows.First(r => r.Id == plainDeletedJobId);
        plainRow.Cancelled.ShouldBeFalse();
    }

    private Task<HttpResponseMessage> cancel(string jobId, string? reason, string? expectedState)
        => _client.PostAsJsonAsync($"/hangfire/api/runs/{Uri.EscapeDataString(jobId)}/cancel", new { reason, expectedState });

    private static string? getJobState(string jobId)
    {
        using var connection = JobStorage.Current.GetConnection();
        return connection.GetJobData(jobId)?.State;
    }

    private static async Task<string?> waitForState(string jobId, string state)
    {
        var deadline = DateTime.UtcNow + PollTimeout;
        string? current;
        do
        {
            current = getJobState(jobId);
            if (string.Equals(current, state, StringComparison.OrdinalIgnoreCase)) return current;
            await Task.Delay(PollInterval);
        } while (DateTime.UtcNow < deadline);
        return current;
    }

    private async Task<AuditEntryDto?> waitForAuditAction(string jobId, string action)
    {
        var deadline = DateTime.UtcNow + PollTimeout;
        AuditEntryDto? match;
        do
        {
            match = (await getRunsAudit(jobId)).FirstOrDefault(e => e.Action == action);
            if (match != null) return match;
            await Task.Delay(PollInterval);
        } while (DateTime.UtcNow < deadline);
        return match;
    }

    private async Task<List<AuditEntryDto>> getRunsAudit(string jobId)
    {
        var response = await _client.GetAsync($"/hangfire/api/runs/{Uri.EscapeDataString(jobId)}/audit");
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<List<AuditEntryDto>>(JsonOptions))!;
    }

    private async Task<List<RunDeletedSummaryDto>> getDeletedList()
    {
        var response = await _client.GetAsync("/hangfire/api/runs/deleted?count=500");
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<List<RunDeletedSummaryDto>>(JsonOptions))!;
    }

    // Local mirror of RunEndpoints.RunDeletedSummary — same reasoning as the other test classes' local
    // wire-shape mirrors: this test only depends on the wire shape, the way an external consumer would.
    private sealed record RunDeletedSummaryDto(
        string Id, string? JobDisplayName, DateTime? DeletedAt,
        bool Cancelled, string? CancelledBy, DateTime? CancelledAt, string? CancelReason);

    // Local mirror of JobControlEndpoints.AuditEntry — same reasoning as JobControlApiTests' own copy.
    private sealed record AuditEntryDto(
        int V, DateTime At, string Actor, string Action, string JobId, string? Reason, string Outcome, Dictionary<string, string>? Detail);
}
