using System.Text.Json;

namespace Hangfire.OpsToolkit.JobControl;

/// <summary>
/// One recorded action against the job-control surface: a human operation in Phase 1 (disable / enable
/// / trigger / delete), and in later phases a system-detected definition change or terminal
/// state-transition. <see cref="V"/> is the stored schema version, carried on every entry so a future
/// release can add fields without an old binary choking on a newer entry, or vice versa — see
/// <see cref="TryDeserialize"/>'s parse-tolerant contract.
/// </summary>
public sealed record AuditEntry(
    int V,
    DateTime At,
    string Actor,
    string Action,
    string JobId,
    string? Reason,
    string Outcome,
    IReadOnlyDictionary<string, string>? Detail)
{
    public const int CurrentVersion = 1;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public static string Serialize(AuditEntry entry) => JsonSerializer.Serialize(entry, JsonOptions);

    /// <summary>
    /// Returns null for a line that fails to parse, rather than throwing — a single corrupt or
    /// foreign entry must not take down the whole history read. Deliberately broad catch: any failure
    /// mode here (malformed JSON, a shape from some future/foreign writer) is equally "skip it".
    /// </summary>
    public static AuditEntry? TryDeserialize(string json)
    {
        try
        {
            return JsonSerializer.Deserialize<AuditEntry>(json, JsonOptions);
        }
        catch
        {
            return null;
        }
    }
}
