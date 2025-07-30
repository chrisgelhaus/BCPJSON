# BCPJSON

After reading an article about how Facebook performs their MySQL backups at scale, I wanted to see if it was possible to replicate part of that model with Microsoft SQL Server. BCPJSON is the result of that experiment. The goal of the tool is to unload data into a structured format so that archives can easily be compared or imported elsewhere.

Three verbs are supported:

* `export` – dump tables from SQL Server into either **BCP** or **JSON** files
* `import` – load a previously exported BCP file into a SQL Server table
* `copy` – copy tables directly from one SQL Server instance to another using `SqlBulkCopy`

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

### Copy

```
dotnet run -- copy --source-server SourceSql --source-database SrcDb \
    --target-server DestSql --target-database DestDb
```

If no path is provided to the export or import commands, the current working directory is used.

BCPJSON writes colorized output to the console and optionally logs messages to a file when the `--log-file` option is specified.  Only a subset of the available command line switches are shown above; run the program with `--help` on any verb to see the full list of options.
