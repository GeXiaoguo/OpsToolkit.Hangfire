# Hangfire.OpsToolkit

Production controls for Hangfire recurring jobs—without adding a database schema, a frontend build,
or host-specific infrastructure.

`Hangfire.OpsToolkit.JobControl` gives operators a safe way to disable, enable, trigger, inspect, and
delete recurring jobs. Every human action is recorded in a durable audit trail, and the package ships
with a self-contained operator UI that plugs into an existing ASP.NET Core host.

## Why use it?

Hangfire's dashboard is excellent for observing background work, but production teams often need a
stronger operational boundary around recurring jobs. JobControl adds that boundary while staying
close to Hangfire's own storage model:

- **Durable disable and enable:** disabled state survives application restarts and
  `RecurringJob.AddOrUpdate` registrations.
- **Operator accountability:** audit entries capture actor, action, job, reason, timestamp, and
  outcome.
- **Authorization by construction:** separate ASP.NET Core policies protect viewing and management.
- **Zero-schema adoption:** state is stored in Hangfire's existing storage primitives—no migrations
  or extra database tables.
- **Bundled UI:** one embedded HTML resource, with no npm toolchain or static-file setup.
- **Storage-provider friendly:** the package depends on `Hangfire.Core`, not a particular SQL or
  storage provider.

## Install

```bash
dotnet add package Hangfire.OpsToolkit.JobControl
```

> The package will become available after the first nuget.org release. Until then, clone this
> repository and reference the project directly.

## Quick start

Register the server-side filter with Hangfire:

```csharp
builder.Services.AddHangfire(configuration => configuration
    .UsePostgreSqlStorage(/* your existing storage configuration */)
    .UseJobControl());
```

Map the API and UI using authorization policies from your application:

```csharp
app.MapJobControl(
    viewPolicy: "CanViewJobs",
    managePolicy: "CanManageJobs");
```

Then open `/hangfire/job-control` in your host. The default endpoints are:

| Method | Route | Purpose |
|---|---|---|
| `GET` | `/hangfire/api/recurring` | List recurring jobs and operational state |
| `POST` | `/hangfire/api/recurring/{jobId}/disable` | Disable a job with a reason |
| `POST` | `/hangfire/api/recurring/{jobId}/enable` | Re-enable a job |
| `POST` | `/hangfire/api/recurring/{jobId}/trigger` | Trigger a job now |
| `POST` | `/hangfire/api/recurring/{jobId}/delete` | Delete a recurring job |
| `GET` | `/hangfire/api/recurring/audit` | Read operator-action history |
| `GET` | `/hangfire/job-control` | Open the bundled operator UI |

Customize actor identity and audit retention when mapping the endpoints:

```csharp
app.MapJobControl(
    viewPolicy: "CanViewJobs",
    managePolicy: "CanManageJobs",
    options: new JobControlOptions
    {
        ActorProvider = context =>
            context.User.FindFirst("email")?.Value ?? "unknown",
        AuditMaxEntries = 10_000,
        AuditDefaultReadLimit = 200,
    });
```

## Design philosophy

OpsToolkit is intentionally modular: applications install only the operational capabilities they
need. JobControl keeps its dependency surface small—`Hangfire.Core`, the ASP.NET Core shared
framework, and the .NET base libraries—and does not couple consumers to the demo host or a storage
provider.

The authorization policies are required arguments. This makes accidentally mapping management
endpoints without an authorization gate harder than doing the secure thing.

## Try the demo

The included host uses PostgreSQL for realistic storage and HTTP integration testing:

```bash
cd tests/Hangfire.OpsToolkit.Host
docker compose -f docker-compose.postgres.yaml up -d
dotnet run
```

Open `/hangfire` for the standard dashboard or `/hangfire/job-control` for JobControl.

## Build and test

With the demo PostgreSQL container running:

```bash
dotnet restore Hangfire.OpsToolkit.sln
dotnet build Hangfire.OpsToolkit.sln --configuration Release --no-restore
dotnet test Hangfire.OpsToolkit.sln --configuration Release --no-build
```

## Community

Bug reports, compatibility findings, documentation improvements, and focused feature proposals are
welcome through GitHub Issues. If you run Hangfire in a production or regulated environment, sharing
the operational problem you are solving is especially valuable—it helps keep the toolkit practical
and provider-neutral.

Hangfire.OpsToolkit is an independent community project and is not affiliated with or endorsed by
Hangfire OÜ.

## License

MIT. See [LICENSE](LICENSE).
