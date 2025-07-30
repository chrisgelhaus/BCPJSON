# BCPJSON

After reading an article about how Facebook performs their MySQL backups at scale, I wanted to see if it was possible to replicate part of that model with Microsoft SQL Server.
I set out to see if I could build a tool to unload data into a structured format that would allow for DIFF operations against the resulting archive file.

# Resources

## Recent Updates
- Added optional logging to file when `--log-file` is supplied.
- Copy operations now accept a `--batch-size` argument to tune bulk copy performance.
- Export and import commands validate file paths and create export folders when needed.
