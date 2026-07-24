using Hangfire.Common;
using Shouldly;
using Xunit;

namespace OpsToolkit.Hangfire.JobControl.Tests;

// Pure — no storage, no Hangfire calls. JobArgs derives the operator-facing schema and binds
// operator JSON back to positional arguments; the executing sides (RecurringJobRegistrar.Apply and
// the args/invoke endpoints) are covered by RecurringJobRuntimeTests / ParameterOverrideApiTests.
public class JobArgsTests
{
    private static RecurringJobTypeDefinition sweepDefinition()
        => new("sweep", Job.FromExpression<TestJob>(x => x.Sweep(30, false, Priority.Normal, CancellationToken.None)),
            "0 3 * * *", TimeZoneInfo.Utc);

    private static RecurringJobTypeDefinition typedDefinition()
        => new("typed", Job.FromExpression<TestJob>(x => x.Typed("plenti", 0.5m, default, TimeSpan.Zero, Guid.Empty, null)),
            "0 3 * * *", TimeZoneInfo.Utc);

    [Fact]
    public void Schema_DerivesNamesTypesAndExpressionBakedDefaults_Test()
    {
        var schema = JobArgs.Schema(sweepDefinition());

        schema.Count.ShouldBe(4);
        schema[0].ShouldBe(new JobParameterSchema("daysToKeep", "int", Editable: true, 30, EnumValues: null));
        schema[1].ShouldBe(new JobParameterSchema("dryRun", "bool", Editable: true, false, EnumValues: null));
        schema[2].Name.ShouldBe("priority");
        schema[2].Type.ShouldBe("Priority");
        schema[2].EnumValues.ShouldBe(new[] { "Low", "Normal", "High" });
    }

    [Fact]
    public void Schema_MarksCancellationTokenInjected_NotEditable_Test()
    {
        var token = JobArgs.Schema(sweepDefinition()).Single(p => p.Name == "token");
        token.Editable.ShouldBeFalse();
        token.CodeDefault.ShouldBeNull();
    }

    [Fact]
    public void Bind_NullOrEmptyJson_YieldsCodeDefaults_Test()
    {
        foreach (var argsJson in new string?[] { null, "", "{}" })
        {
            var binding = JobArgs.Bind(sweepDefinition(), argsJson);
            binding.Succeeded.ShouldBeTrue();
            binding.Args![0].ShouldBe(30);
            binding.Args[1].ShouldBe(false);
            binding.Args[2].ShouldBe(Priority.Normal);
            binding.Args[3].ShouldBe(default(CancellationToken)); // injected placeholder, never operator data
        }
    }

    [Fact]
    public void Bind_PartialMap_OverlaysCodeDefaultsPerParameter_Test()
    {
        // A stored override must stay valid when a deploy adds a parameter with a new default — the
        // per-parameter effective = override ?? default rule.
        var binding = JobArgs.Bind(sweepDefinition(), """{"daysToKeep": 7}""");

        binding.Succeeded.ShouldBeTrue();
        binding.Args![0].ShouldBe(7);
        binding.Args[1].ShouldBe(false);
        binding.Args[2].ShouldBe(Priority.Normal);
    }

    [Fact]
    public void Bind_CoercesEveryPrimitiveKind_Test()
    {
        var binding = JobArgs.Bind(typedDefinition(), """
            {
              "name": "updated",
              "rate": 1.25,
              "asOf": "2026-07-01T09:30:00Z",
              "window": "1.02:30:00",
              "batch": "8b56f8ea-9134-44a3-a2a1-2f412e3ac2f3",
              "note": null
            }
            """);

        binding.Succeeded.ShouldBeTrue();
        binding.Args![0].ShouldBe("updated");
        binding.Args[1].ShouldBe(1.25m);
        binding.Args[2].ShouldBe(DateTime.Parse("2026-07-01T09:30:00Z", null, System.Globalization.DateTimeStyles.RoundtripKind));
        binding.Args[3].ShouldBe(new TimeSpan(1, 2, 30, 0));
        binding.Args[4].ShouldBe(Guid.Parse("8b56f8ea-9134-44a3-a2a1-2f412e3ac2f3"));
        binding.Args[5].ShouldBeNull();
    }

    [Fact]
    public void Bind_EnumByName_IsCaseInsensitive_AndRejectsUndefinedNames_Test()
    {
        JobArgs.Bind(sweepDefinition(), """{"priority": "high"}""").Args![2].ShouldBe(Priority.High);

        var binding = JobArgs.Bind(sweepDefinition(), """{"priority": "Urgent"}""");
        binding.Succeeded.ShouldBeFalse();
        binding.Errors.Single().ShouldContain("Low, Normal, High");
    }

    [Fact]
    public void Bind_ParameterNames_AreCaseInsensitive_Test()
    {
        // The schema shows exact C# names, but a hand-written curl body shouldn't fail on casing —
        // there is no ambiguity to protect (names are unique per signature).
        JobArgs.Bind(sweepDefinition(), """{"DaysToKeep": 7}""").Succeeded.ShouldBeTrue();
    }

    [Fact]
    public void Bind_UnknownParameter_IsAnError_Test()
    {
        // The param-removed/renamed detection: after a deploy changes the signature, a stored key no
        // parameter matches must invalidate the row, not silently drop the value.
        var binding = JobArgs.Bind(sweepDefinition(), """{"retentionDays": 7}""");
        binding.Succeeded.ShouldBeFalse();
        binding.Args.ShouldBeNull(); // never a partially-bound array
        binding.Errors.Single().ShouldContain("unknown parameter 'retentionDays'");
    }

    [Theory]
    [InlineData("""{"daysToKeep": "7"}""", "expected a JSON number")]     // type mismatch (strict kinds)
    [InlineData("""{"daysToKeep": null}""", "null is not valid")]          // null for non-nullable value type
    [InlineData("""{"daysToKeep": 2147483648}""", "out of range")]         // overflow
    [InlineData("""{"dryRun": "yes"}""", "expected a JSON boolean")]
    [InlineData("""{"token": true}""", "injected by the server")]          // injected params reject values
    [InlineData("""[1, 2]""", "not a JSON object")]                        // wrong root shape
    [InlineData("""{"daysToKeep": }""", "not a JSON object")]              // malformed JSON
    public void Bind_CollectsCoercionErrors_Test(string argsJson, string expectedError)
    {
        var binding = JobArgs.Bind(sweepDefinition(), argsJson);
        binding.Succeeded.ShouldBeFalse();
        binding.Errors.ShouldContain(error => error.Contains(expectedError));
    }

    [Fact]
    public void Bind_ReportsEveryProblemAtOnce_Test()
    {
        var binding = JobArgs.Bind(sweepDefinition(), """{"daysToKeep": "7", "dryRun": 1, "ghost": true}""");
        binding.Errors.Count.ShouldBe(3);
    }

    [Fact]
    public void Bind_ComplexParameter_RoundTripsAsJson_Test()
    {
        var definition = new RecurringJobTypeDefinition(
            "complex", Job.FromExpression<TestJob>(x => x.Complex(new[] { 1, 2 })), "0 3 * * *", TimeZoneInfo.Utc);

        var binding = JobArgs.Bind(definition, """{"ids": [3, 4, 5]}""");
        binding.Succeeded.ShouldBeTrue();
        binding.Args![0].ShouldBe(new[] { 3, 4, 5 });

        JobArgs.Bind(definition, """{"ids": "3,4,5"}""").Succeeded.ShouldBeFalse();
    }

    public enum Priority { Low, Normal, High }

    public class TestJob
    {
        public void Sweep(int daysToKeep, bool dryRun, Priority priority, CancellationToken token) { }

        public void Typed(string name, decimal rate, DateTime asOf, TimeSpan window, Guid batch, string? note) { }

        public void Complex(int[] ids) { }
    }
}
