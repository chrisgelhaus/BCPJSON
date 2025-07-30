# BCPJSON

After reading an article about how Facebook performs their MySQL backups at scale, I wanted to see if it was possible to replicate part of that model with Microsoft SQL Server. I set out to build a tool to unload data into a structured format that would allow for DIFF operations against the resulting archive file.

## Usage

The application is a console program targeting **.NET 8.0**. Build it using the `dotnet` CLI:

```bash
dotnet build
```

### Export

```
dotnet run -- export --source-server localhost --source-database MyDb \
    --file-format json --export-path ./output
```

### Import

```
dotnet run -- import --file-format bcp --source-path ./output \
    --source-file data.bcp --target-server localhost
```

If no path is provided, the current working directory is used.

# Resources
