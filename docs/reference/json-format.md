# JSON format

Each JSON export file is a JSON array with one object per row, one row per line, so ordinary line-based diff tools compare two exports row by row:

```json
[{"CustomerID":1,"Name":"Contoso","Created":"2024-01-15T09:30:00","Logo":null}
,{"CustomerID":2,"Name":"Fabrikam","Created":"2024-02-01T00:00:00","Logo":"iVBORw0KGgo="}
]
```

- **Row order.** Rows are ordered by the primary key. A table without one is ordered by every sortable column. Repeated exports of unchanged data are therefore identical.
- **Dates and times** are ISO 8601. `datetimeoffset` keeps its offset.
- **`decimal` and `numeric`** are JSON numbers with every digit, up to the full 38 digits of precision (more than .NET's `decimal` holds), and are read back exactly. A JSON parser in another tool may round very long numbers.
- **`binary`, `varbinary` and `image`** are base64.
- **geography, geometry and hierarchyid** are written as text (`POINT (-122.3 47.6)`, `/1/3/`) and parsed back on import.
  - A geography or geometry value whose SRID is not the default (4326 for geography, 0 for geometry) gets an SRID prefix, as in PostGIS: `SRID=27700;POINT (530000 180000)`. Import rebuilds the value with that SRID.
  - Values with the default SRID have no prefix, so they look exactly as in older exports, and older files import as before.
- **NULL** is written explicitly as `null`.
- **`sql_variant`** values are re-imported as `nvarchar`.

With `--compress gzip` the same content is written gzip-compressed, as `<name>.json.gz`.
