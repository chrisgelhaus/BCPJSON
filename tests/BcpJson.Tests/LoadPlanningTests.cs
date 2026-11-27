using Xunit;

namespace bcpJson.Tests
{
    public class LoadPlanningTests
    {
        private static TargetShape Heap(int nc) => new TargetShape(false, false, nc, false);
        private static TargetShape Clustered(int nc) => new TargetShape(true, false, nc, false);
        private static TargetShape Columnstore(int nc) => new TargetShape(false, true, nc, false);

        private static HeapLoadFacts Facts(Func<HeapLoadFacts, HeapLoadFacts> change = null)
        {
            var f = new HeapLoadFacts(true, false, false, null, null, null, null, false, false, false, false, false, false, true);
            return change == null ? f : change(f);
        }

        public static IEnumerable<object[]> HeapDecisions()
        {
            yield return new object[] { Facts(), true, false, false, null, true, null };
            yield return new object[] { Facts(f => f with { ClusteredRowstore = false }), true, false, false, null, false, null };
            yield return new object[] { Facts(f => f with { ClusteredRowstore = false, ClusteredColumnstore = true }), true, false, false, null, false, "clustered columnstore" };
            yield return new object[] { Facts(), false, false, false, null, false, "--no-table-lock" };
            yield return new object[] { Facts(), true, true, false, null, false, "CLR columns" };
            yield return new object[] { Facts(f => f with { MemoryOptimized = true }), true, false, false, null, false, "memory-optimized" };
            yield return new object[] { Facts(f => f with { ReferencingForeignKey = "FK_X" }), true, false, false, null, false, "referenced by foreign key FK_X" };
            yield return new object[] { Facts(f => f with { XmlOrSpatialIndex = "SIX_G" }), true, false, false, null, false, "XML or spatial index SIX_G" };
            yield return new object[] { Facts(f => f with { DisabledIndex = "IX_Off" }), true, false, false, null, false, "disabled index IX_Off" };
            yield return new object[] { Facts(f => f with { IndexedView = "dbo.V" }), true, false, false, null, false, "indexed view dbo.V" };
            yield return new object[] { Facts(f => f with { Temporal = true }), true, false, false, null, false, "temporal table" };
            yield return new object[] { Facts(f => f with { ChangeTracking = true }), true, false, false, null, false, "change tracking" };
            yield return new object[] { Facts(f => f with { Replicated = true }), true, false, false, null, false, "replication" };
            yield return new object[] { Facts(f => f with { ChangeDataCapture = true }), true, false, false, null, false, "change data capture" };
            yield return new object[] { Facts(f => f with { Filestream = true }), true, false, false, null, false, "FILESTREAM" };
            yield return new object[] { Facts(f => f with { FullText = true }), true, false, false, null, false, "full-text index" };
            yield return new object[] { Facts(f => f with { Empty = false }), true, false, false, null, false, "not empty" };
            yield return new object[] { Facts(f => f with { Empty = false }), true, false, true, null, true, null };
            yield return new object[] { Facts(), true, false, false, "IX_Long", false, "index definition too long: IX_Long" };
        }

        [Theory]
        [MemberData(nameof(HeapDecisions))]
        public void Heap_load_decision(HeapLoadFacts facts, bool tableLock, bool staged, bool truncate, string tooLong, bool expected, string reason)
        {
            var d = LoadPlanning.DecideHeapLoad(facts, tableLock, staged, truncate, tooLong);
            Assert.Equal(expected, d.LoadAsHeap);
            Assert.Equal(reason, d.Reason);
        }

        [Fact]
        public void Heap_load_reason_precedence()
        {
            var both = Facts(f => f with { MemoryOptimized = true, ReferencingForeignKey = "FK_X" });
            Assert.Equal("--no-table-lock", LoadPlanning.DecideHeapLoad(both, false, false, false, null).Reason);
            Assert.Equal("memory-optimized", LoadPlanning.DecideHeapLoad(both, true, false, false, null).Reason);
            var dis = Facts(f => f with { DisabledIndex = "IX_Off", IndexedView = "dbo.V" });
            Assert.Equal("disabled index IX_Off", LoadPlanning.DecideHeapLoad(dis, true, false, false, null).Reason);
        }

        public static IEnumerable<object[]> Modes()
        {
            LoadMode cci = LoadPlanning.ClusteredColumnstoreLoadsInParallel ? LoadMode.Parallel : LoadMode.Sequential;
            yield return new object[] { Heap(0), true, false, LoadMode.Parallel, null };
            yield return new object[] { Heap(2), true, false, LoadMode.Sequential, "enabled nonclustered indexes" };
            yield return new object[] { Clustered(0), true, false, LoadMode.Sequential, "clustered index" };
            yield return new object[] { Columnstore(0), true, false, cci, cci == LoadMode.Parallel ? null : "columnstore" };
            yield return new object[] { Columnstore(1), true, false, LoadMode.Sequential, "enabled nonclustered indexes" };
            yield return new object[] { Clustered(3), false, false, LoadMode.Parallel, null };
            yield return new object[] { Heap(0), true, true, LoadMode.Sequential, "CLR columns" };
            yield return new object[] { Heap(0), false, true, LoadMode.Sequential, "CLR columns" };
            yield return new object[] { new TargetShape(false, false, 0, true), true, false, LoadMode.Parallel, null };
            yield return new object[] { new TargetShape(true, false, 0, true), true, false, LoadMode.Sequential, "clustered index" };
        }

        [Theory]
        [MemberData(nameof(Modes))]
        public void Load_mode(TargetShape shape, bool tableLock, bool staged, LoadMode expected, string reason)
        {
            Assert.Equal(expected, LoadPlanning.ChooseLoadMode(shape, tableLock, staged));
            Assert.Equal(reason, LoadPlanning.SequentialReason(shape, tableLock, staged));
        }

        private static IndexInfo Ix(int id, byte type, bool pk = false, bool disabled = false, bool fk = false, bool secXml = false)
            => new IndexInfo(id, "IX" + id, type, pk, disabled, fk, secXml);

        [Theory]
        [InlineData(false, true, false, false, true)]    // existing, --disable-indexes
        [InlineData(false, false, false, true, true)]    // existing, --load-as-heap fallback (not split or did not qualify)
        [InlineData(false, false, true, true, false)]    // loaded as a heap: indexes are dropped, not disabled
        [InlineData(true, true, false, true, false)]     // created by this copy
        [InlineData(false, false, false, false, false)]  // neither option
        public void Copy_disables_indexes_only_on_existing_targets(bool created, bool disableIndexes, bool loadedAsHeap, bool loadAsHeap, bool expected)
        {
            Assert.Equal(expected, LoadPlanning.CopyDisablesIndexes(created, disableIndexes, loadedAsHeap, loadAsHeap));
        }

        [Fact]
        public void Indexes_to_disable()
        {
            var all = new[]
            {
                Ix(0, 0), Ix(1, 1), Ix(1, 5),
                Ix(2, 2, pk: true),             // nonclustered primary key
                Ix(3, 2, fk: true),             // referenced by a foreign key
                Ix(4, 2, disabled: true),       // already disabled
                Ix(5, 2), Ix(6, 3), Ix(7, 4), Ix(8, 6), Ix(9, 7),
            };
            Assert.Equal(new[] { 5, 6, 7, 8, 9 }, LoadPlanning.IndexesToDisable(all).Select(i => i.IndexId));
        }

        [Fact]
        public void Unique_nonreferenced_index_is_disabled()
        {
            // uniqueness is not part of IndexInfo: a unique nonclustered index is disabled like any other
            Assert.Single(LoadPlanning.IndexesToDisable(new[] { Ix(4, 2) }));
        }

        [Fact]
        public void Rebuild_order_puts_secondary_xml_after_primary_xml()
        {
            var order = LoadPlanning.RebuildOrder(new[] { Ix(2, 3, secXml: true), Ix(5, 3), Ix(3, 2) });
            Assert.Equal(new[] { 3, 5, 2 }, order.Select(i => i.IndexId));
        }
    }
}
