namespace bcpJson
{
    public enum LoadMode { Parallel, Sequential }

    /// <summary>The target table as it is right before loading (after any index disabling).</summary>
    public sealed record TargetShape(bool ClusteredRowstore, bool ClusteredColumnstore, int EnabledNonclusteredIndexes, bool Partitioned);

    /// <summary>One index of the target (sys.indexes), as needed to decide what --disable-indexes disables.</summary>
    public sealed record IndexInfo(int IndexId, string Name, byte Type, bool IsPrimaryKey, bool IsDisabled, bool ReferencedByForeignKey, bool IsSecondaryXml);

    /// <summary>Catalog facts about one target, read once at planning (null name = none).</summary>
    public sealed record HeapLoadFacts(bool ClusteredRowstore, bool ClusteredColumnstore, bool MemoryOptimized,
        string ReferencingForeignKey, string XmlOrSpatialIndex, string DisabledIndex, string IndexedView, bool Temporal,
        bool ChangeTracking, bool Replicated, bool ChangeDataCapture, bool Filestream, bool FullText, bool Empty);

    /// <summary>LoadAsHeap true: drop and reload. False with Reason null: nothing to say (heap target). False with Reason: warn.</summary>
    public sealed record HeapLoadDecision(bool LoadAsHeap, string Reason);

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

        /// <summary>Longest index CREATE statement (characters) that load-as-heap will script.</summary>
        internal const int MaxIndexStatementLength = 3500;

        /// <summary>Whether --load-as-heap applies to a target; the first failing check names the reason.</summary>
        internal static HeapLoadDecision DecideHeapLoad(HeapLoadFacts facts, bool tableLock, bool stagedClrColumns,
            bool truncateTarget, string tooLongIndex)
        {
            if (!facts.ClusteredRowstore && !facts.ClusteredColumnstore) return new HeapLoadDecision(false, null);
            string reason = null;
            if (!tableLock) reason = "--no-table-lock";
            else if (stagedClrColumns) reason = "CLR columns";
            else if (facts.MemoryOptimized) reason = "memory-optimized";
            else if (facts.ClusteredColumnstore) reason = "clustered columnstore";
            else if (facts.ReferencingForeignKey != null) reason = "referenced by foreign key " + facts.ReferencingForeignKey;
            else if (facts.XmlOrSpatialIndex != null) reason = "XML or spatial index " + facts.XmlOrSpatialIndex;
            else if (facts.DisabledIndex != null) reason = "disabled index " + facts.DisabledIndex;
            else if (facts.IndexedView != null) reason = "indexed view " + facts.IndexedView;
            else if (facts.Temporal) reason = "temporal table";
            else if (facts.ChangeTracking) reason = "change tracking";
            else if (facts.Replicated) reason = "replication";
            else if (facts.ChangeDataCapture) reason = "change data capture";
            else if (facts.Filestream) reason = "FILESTREAM";
            else if (facts.FullText) reason = "full-text index";
            else if (!truncateTarget && !facts.Empty) reason = "not empty";
            else if (tooLongIndex != null) reason = "index definition too long: " + tooLongIndex;
            return new HeapLoadDecision(reason == null, reason);
        }

        internal static LoadMode ChooseLoadMode(TargetShape shape, bool tableLock, bool stagedClrColumns)
        {
            return SequentialReason(shape, tableLock, stagedClrColumns) == null ? LoadMode.Parallel : LoadMode.Sequential;
        }

        /// <summary>
        /// Whether copy disables a target's nonclustered indexes: only on a table that existed before the copy, with
        /// --disable-indexes or as the --load-as-heap fallback (a table loaded as a heap has its indexes dropped instead).
        /// </summary>
        internal static bool CopyDisablesIndexes(bool created, bool disableIndexes, bool loadedAsHeap, bool loadAsHeap)
        {
            return !created && !loadedAsHeap && (disableIndexes || loadAsHeap);
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
