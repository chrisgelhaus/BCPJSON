using Xunit;

namespace bcpJson.Tests
{
    public class LoadPlanningTests
    {
        private static TargetShape Heap(int nc) => new TargetShape(false, false, nc, false);
        private static TargetShape Clustered(int nc) => new TargetShape(true, false, nc, false);
        private static TargetShape Columnstore(int nc) => new TargetShape(false, true, nc, false);

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
