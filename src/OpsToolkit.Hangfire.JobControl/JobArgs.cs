using System.Globalization;
using System.Reflection;
using System.Text.Json;
using Hangfire;
using Hangfire.Common;

namespace OpsToolkit.Hangfire.JobControl;

/// <summary>
/// Pure half of parameter overrides: derives a job's operator-facing parameter schema from its
/// <see cref="RecurringJobTypeDefinition"/>, and binds an operator-supplied JSON value map back to a
/// positional argument array for <see cref="Job"/>. No storage, no clock, no Hangfire calls — fully
/// unit-testable; <see cref="RecurringJobRegistrar.Apply"/> and the HTTP endpoints are the executing
/// side.
///
/// The value map is keyed by parameter name and <b>partial by design</b>: a missing key means "use
/// the code default", so a stored override stays valid when a deploy adds a new parameter with a new
/// default — per-parameter <c>effective = override ?? default</c>, the same rule the schedule plane
/// applies to cron. An <i>unknown</i> key is an error, not a silent skip: it is exactly how a
/// parameter rename/removal surfaces after a deploy, and the caller decides whether that means 400
/// (operator input) or soft-invalidation (a stored row — see <see cref="RecurringJobRegistrar"/>).
///
/// Parameters typed <see cref="CancellationToken"/>/<see cref="IJobCancellationToken"/> are
/// <b>injected</b>: Hangfire substitutes the server's shutdown token at perform time regardless of
/// the stored argument (<c>CoreBackgroundJobPerformer.Substitutions</c>), so they are excluded from
/// operator editing and bound to a placeholder the same way Hangfire's own
/// <c>InvocationData.DeserializeArguments</c> does.
/// </summary>
public static class JobArgs
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    // Mirrors CoreBackgroundJobPerformer.Substitutions' key set (verified against Hangfire 1.8):
    // these parameter types are server-provided at perform time, never operator data.
    private static bool isInjected(Type parameterType)
        => parameterType == typeof(CancellationToken) || parameterType == typeof(IJobCancellationToken);

    public static IReadOnlyList<JobParameterSchema> Schema(RecurringJobTypeDefinition definition)
    {
        var parameters = definition.Job.Method.GetParameters();
        var schema = new List<JobParameterSchema>(parameters.Length);
        for (var i = 0; i < parameters.Length; i++)
        {
            var parameter = parameters[i];
            var injected = isInjected(parameter.ParameterType);
            schema.Add(new JobParameterSchema(
                parameter.Name ?? $"arg{i}",
                displayName(parameter.ParameterType),
                Editable: !injected,
                CodeDefault: injected ? null : definition.Job.Args.ElementAtOrDefault(i),
                EnumValues: enumValues(parameter.ParameterType)));
        }
        return schema;
    }

    /// <summary>
    /// Binds <paramref name="argsJson"/> (a JSON object of parameter name → value, or null/empty for
    /// "all code defaults") to the positional argument array for <paramref name="definition"/>'s
    /// method. All problems are collected rather than thrown — the executing side must be able to
    /// report every mismatch at once, and a stored row's failure must never become an exception on
    /// the startup path.
    /// </summary>
    public static JobArgsBinding Bind(RecurringJobTypeDefinition definition, string? argsJson)
    {
        Dictionary<string, JsonElement> values;
        try
        {
            values = parseObject(argsJson);
        }
        catch (JsonException ex)
        {
            return new JobArgsBinding(null, new[] { $"args is not a JSON object: {ex.Message}" });
        }

        var parameters = definition.Job.Method.GetParameters();
        var bound = new object?[parameters.Length];
        var errors = new List<string>();
        var consumed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < parameters.Length; i++)
        {
            var parameter = parameters[i];
            var name = parameter.Name ?? $"arg{i}";

            if (isInjected(parameter.ParameterType))
            {
                // The perform-time substitution ignores this value; store the same placeholder
                // Hangfire's own InvocationData.DeserializeArguments produces.
                bound[i] = parameter.ParameterType.GetTypeInfo().IsValueType
                    ? Activator.CreateInstance(parameter.ParameterType)
                    : null;
                if (values.ContainsKey(name))
                    errors.Add($"parameter '{name}' is injected by the server and cannot be set");
                consumed.Add(name);
                continue;
            }

            if (values.TryGetValue(name, out var element))
            {
                consumed.Add(name);
                if (tryCoerce(element, parameter.ParameterType, out var value, out var error))
                    bound[i] = value;
                else
                    errors.Add($"parameter '{name}': {error}");
            }
            else
            {
                bound[i] = definition.Job.Args.ElementAtOrDefault(i);
            }
        }

        foreach (var key in values.Keys.Where(key => !consumed.Contains(key)))
            errors.Add($"unknown parameter '{key}' — the method has no parameter by that name");

        return errors.Count > 0
            ? new JobArgsBinding(null, errors)
            : new JobArgsBinding(bound, Array.Empty<string>());
    }

    private static Dictionary<string, JsonElement> parseObject(string? argsJson)
    {
        if (string.IsNullOrWhiteSpace(argsJson)) return new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);
        var parsed = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(argsJson, JsonOptions)
            ?? throw new JsonException("JSON null is not a value map.");
        return new Dictionary<string, JsonElement>(parsed, StringComparer.OrdinalIgnoreCase);
    }

    // Primitives get hand-rolled coercion for exact, human-readable error messages; any other type
    // falls through to a general JSON deserialization, so complex parameters (arrays, option
    // objects) are still overridable as raw JSON rather than being locked to their code defaults.
    private static bool tryCoerce(JsonElement element, Type targetType, out object? value, out string? error)
    {
        value = null;
        error = null;

        var underlying = Nullable.GetUnderlyingType(targetType);
        if (element.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            if (underlying is not null || !targetType.GetTypeInfo().IsValueType) return true;
            error = $"null is not valid for non-nullable type {displayName(targetType)}";
            return false;
        }
        var type = underlying ?? targetType;

        if (type == typeof(string))
        {
            if (element.ValueKind == JsonValueKind.String) { value = element.GetString(); return true; }
            return fail(out error, element, "a JSON string");
        }
        if (type == typeof(bool))
        {
            if (element.ValueKind is JsonValueKind.True or JsonValueKind.False) { value = element.GetBoolean(); return true; }
            return fail(out error, element, "a JSON boolean");
        }
        if (type.GetTypeInfo().IsEnum)
        {
            if (element.ValueKind == JsonValueKind.String)
            {
                if (Enum.TryParse(type, element.GetString(), ignoreCase: true, out var parsed) && Enum.IsDefined(type, parsed!))
                {
                    value = parsed;
                    return true;
                }
                error = $"'{element.GetString()}' is not one of: {string.Join(", ", Enum.GetNames(type))}";
                return false;
            }
            return fail(out error, element, $"one of: {string.Join(", ", Enum.GetNames(type))}");
        }
        if (isNumeric(type))
        {
            if (element.ValueKind == JsonValueKind.Number)
            {
                try
                {
                    value = Convert.ChangeType(element.GetDecimal(), type, CultureInfo.InvariantCulture);
                    return true;
                }
                catch (OverflowException)
                {
                    error = $"{element.GetRawText()} is out of range for {displayName(type)}";
                    return false;
                }
            }
            return fail(out error, element, $"a JSON number ({displayName(type)})");
        }
        if (type == typeof(Guid))
        {
            if (element.ValueKind == JsonValueKind.String && Guid.TryParse(element.GetString(), out var guid)) { value = guid; return true; }
            return fail(out error, element, "a GUID string");
        }
        if (type == typeof(DateTime))
        {
            if (element.ValueKind == JsonValueKind.String &&
                DateTime.TryParse(element.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTime))
            {
                value = dateTime;
                return true;
            }
            return fail(out error, element, "an ISO-8601 date-time string");
        }
        if (type == typeof(DateTimeOffset))
        {
            if (element.ValueKind == JsonValueKind.String &&
                DateTimeOffset.TryParse(element.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeOffset))
            {
                value = dateTimeOffset;
                return true;
            }
            return fail(out error, element, "an ISO-8601 date-time string");
        }
        if (type == typeof(TimeSpan))
        {
            if (element.ValueKind == JsonValueKind.String && TimeSpan.TryParse(element.GetString(), CultureInfo.InvariantCulture, out var timeSpan))
            {
                value = timeSpan;
                return true;
            }
            return fail(out error, element, "a time-span string like '1.02:30:00'");
        }

        try
        {
            value = element.Deserialize(type, JsonOptions);
            return true;
        }
        catch (JsonException ex)
        {
            error = $"cannot read {displayName(type)} from {element.GetRawText()} — {ex.Message}";
            return false;
        }
        catch (NotSupportedException ex)
        {
            error = $"type {displayName(type)} is not JSON-bindable — {ex.Message}";
            return false;
        }
    }

    private static bool fail(out string? error, JsonElement element, string expected)
    {
        error = $"expected {expected}, got {element.GetRawText()}";
        return false;
    }

    private static bool isNumeric(Type type)
        => type == typeof(int) || type == typeof(long) || type == typeof(short) || type == typeof(byte)
        || type == typeof(sbyte) || type == typeof(uint) || type == typeof(ulong) || type == typeof(ushort)
        || type == typeof(double) || type == typeof(float) || type == typeof(decimal);

    private static readonly Dictionary<Type, string> Keywords = new()
    {
        [typeof(int)] = "int", [typeof(long)] = "long", [typeof(short)] = "short", [typeof(byte)] = "byte",
        [typeof(sbyte)] = "sbyte", [typeof(uint)] = "uint", [typeof(ulong)] = "ulong", [typeof(ushort)] = "ushort",
        [typeof(double)] = "double", [typeof(float)] = "float", [typeof(decimal)] = "decimal",
        [typeof(bool)] = "bool", [typeof(string)] = "string", [typeof(object)] = "object", [typeof(char)] = "char",
    };

    private static string displayName(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type);
        if (underlying is not null) return displayName(underlying) + "?";
        return Keywords.GetValueOrDefault(type, type.Name);
    }

    private static IReadOnlyList<string>? enumValues(Type parameterType)
    {
        var type = Nullable.GetUnderlyingType(parameterType) ?? parameterType;
        return type.GetTypeInfo().IsEnum ? Enum.GetNames(type) : null;
    }
}

/// <summary>
/// One method parameter as the operator sees it. <see cref="Editable"/> is false for server-injected
/// parameters (cancellation tokens), which carry no <see cref="CodeDefault"/> and reject submitted
/// values. <see cref="Type"/> is a display name ("int", "bool?", "MyEnum"), not a CLR identifier —
/// the UI's label, while <see cref="EnumValues"/> (enum types only) feeds a select control.
/// </summary>
public sealed record JobParameterSchema(
    string Name,
    string Type,
    bool Editable,
    object? CodeDefault,
    IReadOnlyList<string>? EnumValues);

/// <summary>
/// A <see cref="JobArgs.Bind"/> outcome: <see cref="Args"/> is positional and complete when
/// <see cref="Succeeded"/>, null otherwise — never a partially-bound array, so a caller can't
/// accidentally run a job on half-applied values.
/// </summary>
public sealed record JobArgsBinding(IReadOnlyList<object?>? Args, IReadOnlyList<string> Errors)
{
    public bool Succeeded => Errors.Count == 0;
}
