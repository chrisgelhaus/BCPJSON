# BCPJSON

After reading an article about how Facebook performs their MySQL backups at scale, I wanted to see if it was possible to replicate part of that model with Microsoft SQL Server.
I set out to see if I could build a tool to unload data into a structured format that would allow for DIFF operations against the resulting archive file.

# Resources

## Logging
Command options now support a `--log-file` argument to write console output to a file.

## Validation
Export and import commands verify that the specified paths exist. Export will create the target directory if needed.
