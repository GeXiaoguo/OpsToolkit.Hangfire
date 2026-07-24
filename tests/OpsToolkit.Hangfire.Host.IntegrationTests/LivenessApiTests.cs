using System.Net.Http.Json;
using System.Text.Json;
using Hangfire;
using Microsoft.AspNetCore.Mvc.Testing;
using Shouldly;
using Xunit;

namespace OpsToolkit.Hangfire.Host.IntegrationTests;

// Drives the liveness beat contract (HeartbeatAttribute, LivenessFilter, context.Beat()) end-to-end
// against the real sample host and asserts what /hangfire/api/runs/processing exposes: a contracted
// job carries a beat from the moment it turns Processing (contract start doubles as beat #1), progress
// arrives once the 5s write throttle lets an in-body beat through, and a plain job's row is untouched.
// [Collection("Sample host")]: see SampleHostCollection.
[Collection("Sample host")]
public class LivenessApiTests : IClassFixture<WebApplicationFactory<Program>>
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    private static readonly TimeSpan PollTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(200);

    private readonly HttpClient _client;

    public LivenessApiTests(WebApplicationFactory<Program> factory)
    {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task Processing_ContractedJob_CarriesBeatAndProgress_Test()
    {
        var jobId = BackgroundJob.Enqueue<LongRunningJobs>(x => x.SteadyBeats(null!));

        // Beat present as soon as the row is Processing — before any in-body Beat() persisted.
        var row = await waitForProcessingRow(jobId, r => r.Beat is not null);
        row.ShouldNotBeNull();
        row!.Beat!.TimeoutSeconds.ShouldBe(90);
        row.Beat.Overdue.ShouldBeFalse();

        // SteadyBeats runs ~9s; the first in-body beat past the 5s throttle carries progress.
        var progressed = await waitForProcessingRow(jobId, r => r.Beat?.Percent is not null);
        progressed.ShouldNotBeNull();
        progressed!.Beat!.Percent!.Value.ShouldBeInRange(0, 100);
        progressed.Beat.Message.ShouldNotBeNullOrEmpty();
    }

    [Fact]
    public async Task Processing_PlainJob_HasNoBeat_Test()
    {
        var jobId = BackgroundJob.Enqueue<CancellationTestJobs>(x => x.IgnoringLoop());

        var row = await waitForProcessingRow(jobId, _ => true);
        row.ShouldNotBeNull();
        row!.Beat.ShouldBeNull();
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
    private sealed record RunBeatSummaryDto(DateTime LastBeatAt, double? Percent, string? Message, int TimeoutSeconds, bool Overdue);

    private sealed record RunProcessingSummaryDto(string Id, string? JobDisplayName, string? ServerId, DateTime? StartedAt, RunBeatSummaryDto? Beat);
}
