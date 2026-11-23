namespace bcpJson.Tests
{
    public class WorkQueueTests
    {
        [Fact]
        public void Before_runs_once_and_after_once_after_the_last_item()
        {
            int before = 0, after = 0, done = 0, doneAtAfter = -1;
            var work = new TableWork("t", 8, () => Interlocked.Increment(ref before),
                () => { Interlocked.Increment(ref after); doneAtAfter = Volatile.Read(ref done); });

            WorkQueue.RunLargestFirst(Enumerable.Range(0, 8), i => i, 4,
                i => work.Run(() => Interlocked.Increment(ref done)));

            Assert.Equal(1, before);
            Assert.Equal(1, after);
            Assert.Equal(8, doneAtAfter);
        }

        [Fact]
        public void After_runs_even_when_an_item_throws()
        {
            int after = 0;
            var work = new TableWork("t", 5, null, () => Interlocked.Increment(ref after));

            // Callers catch per item (Parallel.ForEach would stop scheduling after an escaped exception).
            ApplicationException caught = null;
            for (int i = 1; i <= 5; i++)
            {
                int n = i;
                try { work.Run(() => { if (n == 3) throw new ApplicationException("boom"); }); }
                catch (ApplicationException ex) { caught = ex; }
            }

            Assert.NotNull(caught);
            Assert.Equal(1, after);
        }

        [Fact]
        public void Item_exception_wins_over_after_exception()
        {
            var work = new TableWork("t", 1, null, () => throw new InvalidCastException("after"));
            var ex = Assert.Throws<ApplicationException>(() => work.Run(() => throw new ApplicationException("item")));
            Assert.Equal("item", ex.Message);
        }

        [Fact]
        public void Failed_before_skips_items_but_still_runs_after()
        {
            int body = 0, after = 0;
            var work = new TableWork("dbo.T", 3, () => throw new ApplicationException("nope"), () => after++);

            for (int i = 0; i < 3; i++)
            {
                var ex = Assert.Throws<InvalidOperationException>(() => work.Run(() => body++));
                Assert.Contains("preparation failed", ex.Message);
            }

            Assert.Equal(0, body);
            Assert.Equal(1, after);
        }

        [Fact]
        public void Largest_items_start_first()
        {
            var order = new List<int>();
            WorkQueue.RunLargestFirst(new[] { 3, 9, 1, 5 }, i => i, 1, i => order.Add(i));
            Assert.Equal(new[] { 9, 5, 3, 1 }, order);
        }
    }
}
