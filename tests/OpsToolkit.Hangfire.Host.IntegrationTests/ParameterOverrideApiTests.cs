using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Shouldly;
using Xunit;

namespace OpsToolkit.Hangfire.Host.IntegrationTests;

// Drives the parameter-override and invoke endpoints against the real sample host, whose
// "retention-sweep" demo job is registered with expression-baked arguments (30, false) and a
// server-injected CancellationToken — see Program.cs. [Collection("Sample host")]: shares the
// sequential collection with the other host-booting classes.
[Collection("Sample host")]
public class ParameterOverrideApiTests : IClassFixture<WebApplicationFactory<Program>>
{
    private const string JobId = "retention-sweep";

    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private readonly HttpClient _client;

    public ParameterOverrideApiTests(WebApplicationFactory<Program> factory)
    {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task Parameters_CarrySchemaDefaultsAndInjectedFlag_Test()
    {
        var view = await getParameters(JobId);

        view.JobId.ShouldBe(JobId);
        view.Parameters.Count.ShouldBe(3);

        var daysToKeep = view.Parameters.Single(p => p.Name == "daysToKeep");
        daysToKeep.Type.ShouldBe("int");
        daysToKeep.Editable.ShouldBe(true);
        daysToKeep.CodeDefault!.Value.GetInt32().ShouldBe(30); // the expression-baked value

        var dryRun = view.Parameters.Single(p => p.Name == "dryRun");
        dryRun.Type.ShouldBe("bool");
        dryRun.CodeDefault!.Value.GetBoolean().ShouldBe(false);

        var token = view.Parameters.Single(p => p.Name == "token");
        token.Editable.ShouldBe(false); // server-injected — never operator data
    }

    [Fact]
    public async Task Parameters_ParameterlessDeclaredJob_HasEmptySchema_Test()
    {
        (await getParameters("nightly-report")).Parameters.ShouldBeEmpty();
    }

    [Fact]
    public async Task Parameters_UndeclaredJob_ReturnsNotFound_Test()
    {
        (await _client.GetAsync($"/hangfire/api/recurring/dashboard-pilot-job/parameters"))
            .StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task ArgsOverride_AppliesImmediately_PersistsAndResets_Test()
    {
        try
        {
            await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/args/reset", new { reason = "test setup" });

            var reason = $"args override test {Guid.NewGuid()}";
            (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/args",
                new { args = new { daysToKeep = 7, dryRun = true }, reason }))
                .EnsureSuccessStatusCode();

            var job = await getJob(JobId);
            job.Override.ShouldNotBeNull();
            job.Override!.ArgsJson.ShouldNotBeNull();
            job.Override.Cron.ShouldBeNull();  // a parameter override alone is not a schedule override
            job.Override.Reason.ShouldBe(reason);
            string.IsNullOrEmpty(job.Override.InvalidatedAt).ShouldBeTrue();

            var view = await getParameters(JobId);
            var daysToKeep = view.Parameters.Single(p => p.Name == "daysToKeep");
            daysToKeep.OverrideValue!.Value.GetInt32().ShouldBe(7);
            daysToKeep.Effective!.Value.GetInt32().ShouldBe(7);
            daysToKeep.CodeDefault!.Value.GetInt32().ShouldBe(30);

            var overrideEntry = (await getAudit(JobId, limit: 1)).Single();
            overrideEntry.Action.ShouldBe("args-override");
            overrideEntry.Outcome.ShouldBe("ok");
            overrideEntry.Detail!["OldArgs"].ShouldBe("");
            overrideEntry.Detail["NewArgs"].ShouldContain("\"daysToKeep\":7");

            (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/args/reset", new { reason = "back to normal" }))
                .EnsureSuccessStatusCode();

            (await getJob(JobId)).Override.ShouldBeNull();
            (await getParameters(JobId)).Parameters.Single(p => p.Name == "daysToKeep").Effective!.Value.GetInt32().ShouldBe(30);

            var resetEntry = (await getAudit(JobId, limit: 1)).Single();
            resetEntry.Action.ShouldBe("args-reset");
            resetEntry.Detail!["RestoredArgs"].ShouldContain("daysToKeep");
        }
        finally
        {
            await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/args/reset", new { reason = "test cleanup" });
        }
    }

    [Fact]
    public async Task ArgsOverride_UnknownOrMistypedValues_AreRejectedAndNotPersisted_Test()
    {
        await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/args/reset", new { reason = "test setup" });

        (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/args",
            new { args = new { retentionDays = 7 }, reason = "test" }))
            .StatusCode.ShouldBe(HttpStatusCode.BadRequest);

        (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/args",
            new { args = new { daysToKeep = "seven" }, reason = "test" }))
            .StatusCode.ShouldBe(HttpStatusCode.BadRequest);

        (await getJob(JobId)).Override.ShouldBeNull(); // bind-first: nothing was written
    }

    [Fact]
    public async Task ArgsOverride_MissingReason_ReturnsBadRequest_Test()
    {
        (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/args",
            new { args = new { daysToKeep = 7 }, reason = (string?)null }))
            .StatusCode.ShouldBe(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task CronReset_PreservesArgsOverride_AndViceVersa_Test()
    {
        // The row carries both override kinds; resetting one must not destroy the other.
        try
        {
            (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/cron",
                new { cron = "0 4 * * *", reason = "both-override test" })).EnsureSuccessStatusCode();
            (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/args",
                new { args = new { daysToKeep = 9 }, reason = "both-override test" })).EnsureSuccessStatusCode();

            var job = await getJob(JobId);
            job.Cron.ShouldBe("0 4 * * *");
            job.Override!.ArgsJson!.ShouldContain("\"daysToKeep\":9");

            (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/cron/reset", new { reason = "cron only" }))
                .EnsureSuccessStatusCode();

            job = await getJob(JobId);
            job.Cron.ShouldBe("0 2 * * *");                       // Cron.Daily(2) — code default restored
            job.Override.ShouldNotBeNull();                        // row survives on the args override
            job.Override!.Cron.ShouldBeNull();
            job.Override.ArgsJson!.ShouldContain("\"daysToKeep\":9");
            (await getParameters(JobId)).Parameters.Single(p => p.Name == "daysToKeep").Effective!.Value.GetInt32().ShouldBe(9);

            (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/args/reset", new { reason = "args too" }))
                .EnsureSuccessStatusCode();
            (await getJob(JobId)).Override.ShouldBeNull();         // last field cleared removes the row
        }
        finally
        {
            await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/cron/reset", new { reason = "test cleanup" });
            await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/args/reset", new { reason = "test cleanup" });
        }
    }

    [Fact]
    public async Task Invoke_RunsOneOff_WithoutPersisting_AndIsAudited_Test()
    {
        await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/args/reset", new { reason = "test setup" });

        var response = await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/invoke",
            new { args = new { daysToKeep = 1 } });
        response.EnsureSuccessStatusCode();
        var body = (await response.Content.ReadFromJsonAsync<InvokeResponseDto>(JsonOptions))!;
        body.BackgroundJobId.ShouldNotBeNullOrEmpty();

        var entry = (await getAudit(JobId, limit: 1)).Single();
        entry.Action.ShouldBe("invoke");
        entry.Outcome.ShouldBe("ok");
        entry.Detail!["BackgroundJobId"].ShouldBe(body.BackgroundJobId);
        entry.Detail["Args"].ShouldContain("\"daysToKeep\":1");
        entry.Detail["Persisted"].ShouldBe("false");

        (await getJob(JobId)).Override.ShouldBeNull(); // one-off — nothing stored
    }

    [Fact]
    public async Task Invoke_Persist_StoresTheValuesUsed_Test()
    {
        try
        {
            await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/args/reset", new { reason = "test setup" });

            var response = await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/invoke",
                new { args = new { daysToKeep = 2 }, persist = true, reason = "keep these" });
            response.EnsureSuccessStatusCode();

            var job = await getJob(JobId);
            job.Override.ShouldNotBeNull();
            job.Override!.ArgsJson!.ShouldContain("\"daysToKeep\":2");

            // Two audit entries: the invoke itself, and the args-override write it performed.
            var entries = await getAudit(JobId, limit: 2);
            entries.Select(e => e.Action).ShouldBe(new[] { "args-override", "invoke" }, ignoreOrder: true);
            entries.Single(e => e.Action == "invoke").Detail!["Persisted"].ShouldBe("true");
        }
        finally
        {
            await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/args/reset", new { reason = "test cleanup" });
        }
    }

    [Fact]
    public async Task Invoke_PersistWithoutReason_ReturnsBadRequest_Test()
    {
        (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/invoke",
            new { args = new { daysToKeep = 3 }, persist = true }))
            .StatusCode.ShouldBe(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Invoke_BadArgs_ReturnsBadRequest_Test()
    {
        (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/invoke",
            new { args = new { ghost = true } }))
            .StatusCode.ShouldBe(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Invoke_DisabledJob_StillExecutes_Test()
    {
        // The force-run contract: "Trigger now" is skipped for a disabled job (it carries
        // RecurringJobId), while invoke creates an ad-hoc run that executes to Succeeded.
        try
        {
            (await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/disable", new { reason = "force-run test" }))
                .EnsureSuccessStatusCode();

            var response = await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/invoke", new { });
            response.EnsureSuccessStatusCode();
            var backgroundJobId = (await response.Content.ReadFromJsonAsync<InvokeResponseDto>(JsonOptions))!.BackgroundJobId;

            var finalState = await waitForTerminalState(backgroundJobId, TimeSpan.FromSeconds(20));
            finalState.ShouldBe("Succeeded"); // a disable-skip would land in Deleted instead
        }
        finally
        {
            await _client.PostAsJsonAsync($"/hangfire/api/recurring/{JobId}/enable", new { reason = "test cleanup" });
        }
    }

    private async Task<string?> waitForTerminalState(string backgroundJobId, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var response = await _client.GetAsync($"/hangfire/api/runs/{backgroundJobId}");
            if (response.IsSuccessStatusCode)
            {
                var details = (await response.Content.ReadFromJsonAsync<RunJobDetailsDto>(JsonOptions))!;
                var latest = details.History.FirstOrDefault()?.StateName ?? details.History.LastOrDefault()?.StateName;
                if (latest is "Succeeded" or "Failed" or "Deleted") return latest;
            }
            await Task.Delay(250);
        }
        return null;
    }

    private async Task<RecurringJobSummaryDto> getJob(string jobId)
    {
        var response = await _client.GetAsync("/hangfire/api/recurring");
        response.EnsureSuccessStatusCode();
        var jobs = (await response.Content.ReadFromJsonAsync<List<RecurringJobSummaryDto>>(JsonOptions))!;
        return jobs.Single(j => j.Id == jobId);
    }

    private async Task<ParametersViewDto> getParameters(string jobId)
    {
        var response = await _client.GetAsync($"/hangfire/api/recurring/{jobId}/parameters");
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<ParametersViewDto>(JsonOptions))!;
    }

    private async Task<List<AuditEntryDto>> getAudit(string jobId, int limit)
    {
        var response = await _client.GetAsync($"/hangfire/api/recurring/audit?jobId={Uri.EscapeDataString(jobId)}&limit={limit}");
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<List<AuditEntryDto>>(JsonOptions))!;
    }

    // Wire-shape mirrors, per the JobControlApiTests convention — this test depends only on what an
    // external consumer sees. Value fields stay JsonElement because a parameter value's type varies
    // per parameter.
    private sealed record RecurringJobSummaryDto(string Id, string Cron, OverrideRowDto? Override);

    private sealed record OverrideRowDto(
        string? Cron, string? ArgsJson, string UpdatedBy, string UpdatedAt, string? Reason, string? InvalidatedAt, string? InvalidatedReason);

    private sealed record ParametersViewDto(string JobId, List<ParameterDto> Parameters, OverrideRowDto? Override);

    private sealed record ParameterDto(
        string Name, string Type, bool Editable, List<string>? EnumValues,
        JsonElement? CodeDefault, JsonElement? OverrideValue, JsonElement? Effective);

    private sealed record InvokeResponseDto(string BackgroundJobId);

    private sealed record RunJobDetailsDto(string Id, List<RunStateHistoryDto> History);

    private sealed record RunStateHistoryDto(string StateName, string? Reason, DateTime CreatedAt);

    private sealed record AuditEntryDto(
        int V, DateTime At, string Actor, string Action, string JobId, string? Reason, string Outcome,
        Dictionary<string, string>? Detail);
}
