using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Hangfire;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Microsoft.AspNetCore.Mvc.Testing;
using OpsToolkit.Hangfire.JobControl;
using Shouldly;
using Xunit;

namespace OpsToolkit.Hangfire.Host.IntegrationTests;

// Drives stall detection end-to-end against the real sample host: its StallDetector (2s scan interval,
// see Program.cs) flags a seeded silent execution, /runs/stalled surfaces the flag plus detector
// health, /runs/processing carries the authoritative stalled flag on the row's beat, and
// acknowledge-stall records the operator without clearing the flag. The stalled execution is seeded
// directly in the host's storage — a job placed in a Processing-named state with no queue row (nothing
// a worker can pick up) carrying a 2s-timeout contract snapshot, so the test doesn't have to wait out
// the [Heartbeat] attribute's 60s floor. [Collection("Sample host")]: see SampleHostCollection.
[Collection("Sample host")]
public class StallDetectionApiTests : IClassFixture<WebApplicationFactory<Program>>
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    private static readonly TimeSpan PollTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(250);

    private readonly HttpClient _client;

    public StallDetectionApiTests(WebApplicationFactory<Program> factory)
    {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task StallFlag_Surface_And_Acknowledge_EndToEnd_Test()
    {
        var storage = JobStorage.Current;
        string jobId;
        string executionId;
        using (var connection = storage.GetConnection())
        {
            jobId = connection.CreateExpiredJob(
                Job.FromExpression<LongRunningJobs>(x => x.SlowBatch(null!, CancellationToken.None)),
                new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));
            using (var transaction = connection.CreateWriteTransaction())
            {
                transaction.SetJobState(jobId, new ProcessingLikeState());
                transaction.Commit();
            }

            var now = DateTime.UtcNow;
            executionId = Guid.NewGuid().ToString("N");
            LivenessStore.StartContract(connection, jobId, new BeatRecord(
                BeatRecord.CurrentVersion, executionId, StartedAt: now, TimeoutSeconds: 2,
                ServerId: "itest", Seq: 1, BeatAt: now, Percent: 12.5, Message: "step 1"));
        }

        try
        {
            // The host detector confirms the stall (2s timeout of observation + 2s scan cadence).
            var stalled = await waitForStalled(jobId, item => !item.Acknowledged);
            stalled.ShouldNotBeNull();
            stalled!.ExecutionId.ShouldBe(executionId);
            stalled.TimeoutSeconds.ShouldBe(2);
            stalled.Percent.ShouldBe(12.5);

            // Detector health rides on the same payload — the host's detector renews its lease every scan.
            var payload = await getStalled();
            payload.Detector.Status.ShouldBe("healthy");
            payload.Detector.Servers.ShouldNotBeEmpty();

            // The Processing row's beat carries the authoritative flag for the UI's badge and filter.
            var row = await waitForProcessingRow(jobId, r => r.Beat is { Stalled: true });
            row.ShouldNotBeNull();
            row!.Beat!.Stalled.ShouldBeTrue();
            row.Beat.Acknowledged.ShouldBeFalse();

            // Acknowledge records who/why without clearing the flag (review F8)...
            var response = await _client.PostAsJsonAsync(
                $"/hangfire/api/runs/{jobId}/acknowledge-stall", new { reason = "integration test — investigating" });
            response.StatusCode.ShouldBe(HttpStatusCode.OK);

            var acknowledged = await waitForStalled(jobId, item => item.Acknowledged);
            acknowledged.ShouldNotBeNull();
            acknowledged!.AcknowledgedBy.ShouldNotBeNullOrEmpty();
            acknowledged.AcknowledgeReason.ShouldBe("integration test — investigating");

            // ...and a second acknowledgment is refused rather than silently overwriting the first.
            var again = await _client.PostAsJsonAsync(
                $"/hangfire/api/runs/{jobId}/acknowledge-stall", new { reason = "second operator" });
            again.StatusCode.ShouldBe(HttpStatusCode.Conflict);
        }
        finally
        {
            // Terminal state — the detector's next self-heal pass retires both index entries.
            new BackgroundJobClient(JobStorage.Current).ChangeState(jobId, new DeletedState { Reason = "test cleanup" }, null);
        }
    }

    [Fact]
    public async Task AcknowledgeStall_RequiresStalledFlag_And_Reason_Test()
    {
        // A job that exists but has no stall flag (no liveness contract at all) → 409.
        var jobId = BackgroundJob.Schedule<CancellationTestJobs>(x => x.IgnoringLoop(), TimeSpan.FromHours(1));
        var notStalled = await _client.PostAsJsonAsync(
            $"/hangfire/api/runs/{jobId}/acknowledge-stall", new { reason = "nothing to acknowledge" });
        notStalled.StatusCode.ShouldBe(HttpStatusCode.Conflict);

        // Missing reason → 400 before anything is looked up.
        var noReason = await _client.PostAsJsonAsync(
            $"/hangfire/api/runs/{jobId}/acknowledge-stall", new { reason = "" });
        noReason.StatusCode.ShouldBe(HttpStatusCode.BadRequest);

        // Unknown job → 404 (including storage providers whose ids wouldn't even parse).
        var missing = await _client.PostAsJsonAsync(
            "/hangfire/api/runs/definitely-not-a-job/acknowledge-stall", new { reason = "ghost" });
        missing.StatusCode.ShouldBe(HttpStatusCode.NotFound);

        new BackgroundJobClient(JobStorage.Current).ChangeState(jobId, new DeletedState { Reason = "test cleanup" }, null);
    }

    private async Task<RunStalledResponseDto> getStalled()
    {
        var response = await _client.GetAsync("/hangfire/api/runs/stalled");
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<RunStalledResponseDto>(JsonOptions))!;
    }

    private async Task<RunStalledSummaryDto?> waitForStalled(string jobId, Func<RunStalledSummaryDto, bool> predicate)
    {
        var deadline = DateTime.UtcNow + PollTimeout;
        RunStalledSummaryDto? last = null;
        do
        {
            var payload = await getStalled();
            last = payload.Items.FirstOrDefault(item => item.Id == jobId);
            if (last is not null && predicate(last)) return last;
            await Task.Delay(PollInterval);
        } while (DateTime.UtcNow < deadline);
        return last;
    }

    private async Task<RunProcessingSummaryDto?> waitForProcessingRow(string jobId, Func<RunProcessingSummaryDto, bool> predicate)
    {
        var deadline = DateTime.UtcNow + PollTimeout;
        RunProcessingSummaryDto? last = null;
        do
        {
            var response = await _client.GetAsync("/hangfire/api/runs/processing?count=500");
            response.EnsureSuccessStatusCode();
            var rows = (await response.Content.ReadFromJsonAsync<List<RunProcessingSummaryDto>>(JsonOptions))!;
            last = rows.FirstOrDefault(r => r.Id == jobId);
            if (last is not null && predicate(last)) return last;
            await Task.Delay(PollInterval);
        } while (DateTime.UtcNow < deadline);
        return last;
    }

    // Local mirrors of RunEndpoints' wire shapes — same reasoning as the other test classes' copies.
    private sealed record RunBeatSummaryDto(
        DateTime LastBeatAt, double? Percent, string? Message, int TimeoutSeconds, bool Overdue, bool Stalled, bool Acknowledged);

    private sealed record RunProcessingSummaryDto(
        string Id, string? JobDisplayName, string? ServerId, DateTime? StartedAt, RunBeatSummaryDto? Beat);

    private sealed record RunStalledSummaryDto(
        string Id, string? JobDisplayName, string ExecutionId, string? ServerId, DateTime? StartedAt,
        DateTime StalledAt, DateTime LastBeatAt, long Seq, double? Percent, string? Message, int TimeoutSeconds,
        bool Acknowledged, string? AcknowledgedBy, DateTime? AcknowledgedAt, string? AcknowledgeReason);

    private sealed record RunDetectorServerStatusDto(string ServerId, DateTime LastScanAt, int ScanIntervalSeconds, bool Fresh);

    private sealed record RunDetectorStatusDto(string Status, DateTime? LastScanAt, IReadOnlyList<RunDetectorServerStatusDto> Servers);

    private sealed record RunStalledResponseDto(
        RunDetectorStatusDto Detector, long ActiveContractCount, int AcknowledgedCount, int UnacknowledgedCount,
        IReadOnlyList<RunStalledSummaryDto> Items);

    // A state whose Name is "Processing", applied through the public transaction API — the seeded job
    // must look Processing to the detector without any worker involved (ProcessingState's constructor
    // is internal). No queue row exists, so no worker can ever pick the job up.
    private sealed class ProcessingLikeState : IState
    {
        public string Name => ProcessingState.StateName;
        public string Reason => "Seeded by StallDetectionApiTests";
        public bool IsFinal => false;
        public bool IgnoreJobLoadException => true;

        public Dictionary<string, string> SerializeData() => new()
        {
            ["StartedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow),
            ["ServerId"] = "itest",
            ["WorkerId"] = "1",
        };
    }
}
