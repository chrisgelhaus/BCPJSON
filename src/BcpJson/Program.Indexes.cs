using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace bcpJson
{
    partial class Program
    {
        /// <summary>
        /// The target's shape for <see cref="LoadPlanning.ChooseLoadMode"/>, as it will be once the indexes in
        /// <paramref name="indexIdsToBeDisabled"/> are disabled.
        /// </summary>
        static TargetShape ReadTargetShape(SqlConnection conn, int objectId, ISet<int> indexIdsToBeDisabled)
        {
            const string sql = @"SELECT i.index_id, i.type, i.is_disabled,
       CAST(CASE WHEN EXISTS (SELECT 1 FROM sys.partition_schemes ps WHERE ps.data_space_id = i.data_space_id) THEN 1 ELSE 0 END AS BIT)
FROM sys.indexes i
WHERE i.object_id = @objectId;";

            bool clusteredRowstore = false, clusteredColumnstore = false, partitioned = false;
            int enabledNonclustered = 0;
            using (var cmd = new SqlCommand(sql, conn))
            {
                cmd.Parameters.Add("@objectId", SqlDbType.Int).Value = objectId;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        int indexId = reader.GetInt32(0);
                        byte type = reader.GetByte(1);
                        bool disabled = reader.GetBoolean(2);
                        if (indexId <= 1 && reader.GetBoolean(3))
                        {
                            partitioned = true;
                        }
                        if (type == 1)
                        {
                            clusteredRowstore = true;
                        }
                        else if (type == 5)
                        {
                            clusteredColumnstore = true;
                        }
                        else if (indexId > 1 && !disabled && !indexIdsToBeDisabled.Contains(indexId))
                        {
                            enabledNonclustered++;
                        }
                    }
                }
            }
            return new TargetShape(clusteredRowstore, clusteredColumnstore, enabledNonclustered, partitioned);
        }

        /// <summary>The indexes of a table (not the heap entry), by index id.</summary>
        static List<IndexInfo> ReadIndexes(SqlConnection conn, int objectId)
        {
            const string sql = @"SELECT i.index_id, i.name, i.type, i.is_primary_key, i.is_disabled,
       CAST(CASE WHEN EXISTS (SELECT 1 FROM sys.foreign_keys fk WHERE fk.referenced_object_id = i.object_id AND fk.key_index_id = i.index_id) THEN 1 ELSE 0 END AS BIT),
       CAST(CASE WHEN EXISTS (SELECT 1 FROM sys.xml_indexes x WHERE x.object_id = i.object_id AND x.index_id = i.index_id AND x.using_xml_index_id IS NOT NULL) THEN 1 ELSE 0 END AS BIT)
FROM sys.indexes i
WHERE i.object_id = @objectId
      AND i.index_id > 0
ORDER BY i.index_id;";

            var indexes = new List<IndexInfo>();
            using (var cmd = new SqlCommand(sql, conn))
            {
                cmd.Parameters.Add("@objectId", SqlDbType.Int).Value = objectId;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        indexes.Add(new IndexInfo(reader.GetInt32(0), reader.GetString(1), reader.GetByte(2), reader.GetBoolean(3),
                            reader.GetBoolean(4), reader.GetBoolean(5), reader.GetBoolean(6)));
                    }
                }
            }
            return indexes;
        }

        static void DisableIndexes(SqlConnection conn, string qualifiedName, IEnumerable<IndexInfo> indexes)
        {
            foreach (IndexInfo index in indexes)
            {
                ExecuteSql(conn, "ALTER INDEX " + QuoteName(index.Name) + " ON " + qualifiedName + " DISABLE;");
            }
        }

        /// <summary>
        /// Rebuilds the indexes in <see cref="LoadPlanning.RebuildOrder"/>. An index that fails to rebuild (e.g. a unique
        /// index over duplicate rows) is reported as an error and left disabled; the others are still rebuilt.
        /// Returns the number of failures.
        /// </summary>
        static int RebuildIndexes(SqlConnection conn, string qualifiedName, IEnumerable<IndexInfo> indexes)
        {
            string label = UnquotedLabel(qualifiedName);
            int failures = 0, rebuilt = 0;
            foreach (IndexInfo index in LoadPlanning.RebuildOrder(indexes))
            {
                string statement = "ALTER INDEX " + QuoteName(index.Name) + " ON " + qualifiedName + " REBUILD";
                try
                {
                    ExecuteSql(conn, statement + ";");
                    rebuilt++;
                }
                catch (Exception ex)
                {
                    // The first error says why; SQL Server adds "The statement has been terminated." on a line of its own.
                    string reason = ex is SqlException sqlEx && sqlEx.Errors.Count > 0 ? sqlEx.Errors[0].Message : ex.Message;
                    failures++;
                    PostToConsole(string.Format("{0}: Could not rebuild index {1}: {2}. It is still disabled; run {3}.",
                        label, index.Name, reason.Trim().TrimEnd('.'), statement), InfoLevel.Error);
                }
            }
            PostToConsole(string.Format("{0}: Rebuilt {1} index(es).", label, rebuilt));
            return failures;
        }

        /// <summary>"[schema].[table]" as built with <see cref="QuoteName"/> back to "schema.table" for messages.</summary>
        internal static string UnquotedLabel(string qualifiedName)
        {
            var label = new System.Text.StringBuilder(qualifiedName.Length);
            bool inside = false;
            for (int i = 0; i < qualifiedName.Length; i++)
            {
                char c = qualifiedName[i];
                if (!inside)
                {
                    if (c == '[')
                    {
                        inside = true;
                    }
                    else
                    {
                        label.Append(c);
                    }
                }
                else if (c == ']')
                {
                    if (i + 1 < qualifiedName.Length && qualifiedName[i + 1] == ']')
                    {
                        label.Append(']');
                        i++;
                    }
                    else
                    {
                        inside = false;
                    }
                }
                else
                {
                    label.Append(c);
                }
            }
            return label.ToString();
        }

        /// <summary>sys.tables.lock_escalation_desc: TABLE, AUTO or DISABLE.</summary>
        static string LockEscalation(SqlConnection conn, int objectId)
        {
            using (var cmd = new SqlCommand("SELECT lock_escalation_desc FROM sys.tables WHERE object_id = @objectId;", conn))
            {
                cmd.Parameters.Add("@objectId", SqlDbType.Int).Value = objectId;
                return cmd.ExecuteScalar() as string;
            }
        }

        private static bool IsMemoryOptimized(SqlConnection conn, int objectId)
        {
            using (var cmd = new SqlCommand("SELECT is_memory_optimized FROM sys.tables WHERE object_id = @objectId;", conn))
            {
                cmd.Parameters.Add("@objectId", SqlDbType.Int).Value = objectId;
                return cmd.ExecuteScalar() is bool b && b;
            }
        }
    }
}
