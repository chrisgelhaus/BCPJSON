namespace bcpJson
{
    public enum LoadMode { Parallel, Sequential }

    /// <summary>The target table as it is right before loading (after any index disabling).</summary>
    public sealed record TargetShape(bool ClusteredRowstore, bool ClusteredColumnstore, int EnabledNonclusteredIndexes, bool Partitioned);

    /// <summary>One index of the target (sys.indexes), as needed to decide what --disable-indexes disables.</summary>
    public sealed record IndexInfo(int IndexId, string Name, byte Type, bool IsPrimaryKey, bool IsDisabled, bool ReferencedByForeignKey, bool IsSecondaryXml);

    /// <summary>Pure rules for loading the parts of a table: parallel or sequential, and which indexes to disable.</summary>
    internal static class LoadPlanning
    {
        /// <summary>Whether concurrent TABLOCK bulk loads into a clustered columnstore index do not block each other (confirmed by the CI check).</summary>
        internal const bool ClusteredColumnstoreLoadsInParallel = false;

        /// <summary>Why parts must load one after another: "CLR columns", "clustered index", "enabled nonclustered indexes" or "columnstore"; null when they load in parallel.</summary>
        internal static string SequentialReason(TargetShape shape, bool tableLock, bool stagedClrColumns)
        {
            if (stagedClrColumns) return "CLR columns";   // the staging inserts take TABLOCK whatever the user chose
            if (!tableLock) return null;
            if (shape.ClusteredRowstore) return "clustered index";
            if (shape.EnabledNonclusteredIndexes > 0) return "enabled nonclustered indexes";
            if (shape.ClusteredColumnstore && !ClusteredColumnstoreLoadsInParallel) return "columnstore";
            return null;
        }

        internal static LoadMode ChooseLoadMode(TargetShape shape, bool tableLock, bool stagedClrColumns)
        {
            return SequentialReason(shape, tableLock, stagedClrColumns) == null ? LoadMode.Parallel : LoadMode.Sequential;
        }

        /// <summary>Enabled nonclustered indexes (rowstore, columnstore, XML, spatial, hash) except the primary key and indexes a foreign key references.</summary>
        internal static List<IndexInfo> IndexesToDisable(IEnumerable<IndexInfo> indexes)
        {
            return indexes
                .Where(i => !i.IsDisabled && !i.IsPrimaryKey && !i.ReferencedByForeignKey
                    && (i.Type == 2 || i.Type == 3 || i.Type == 4 || i.Type == 6 || i.Type == 7))
                .ToList();
        }

        /// <summary>Rebuild order: everything but secondary XML indexes first (so a primary XML index precedes them), then by index id.</summary>
        internal static List<IndexInfo> RebuildOrder(IEnumerable<IndexInfo> disabled)
        {
            return disabled.OrderBy(i => i.IsSecondaryXml ? 1 : 0).ThenBy(i => i.IndexId).ToList();
        }
    }
}
