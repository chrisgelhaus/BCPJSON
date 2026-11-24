using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;

namespace bcpJson
{
    /// <summary>
    /// Per-table bookkeeping for work items that may run on several threads: <c>before</c> runs once ahead of the
    /// first item, <c>after</c> runs once when the last of the expected items has finished.
    /// <c>after</c> runs whether or not the items succeeded, so callers track success themselves.
    /// When both an item and <c>after</c> throw, the item's exception wins and <c>after</c>'s is dropped.
    /// </summary>
    internal sealed class TableWork
    {
        private readonly string name;
        private readonly Action after;
        private readonly Lazy<Exception> before;
        private int remaining;

        public TableWork(string name, int items, Action before, Action after)
        {
            if (items < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(items), "A table needs at least one work item.");
            }
            this.name = name;
            this.after = after;
            remaining = items;
            this.before = new Lazy<Exception>(() =>
            {
                try
                {
                    before?.Invoke();
                    return null;
                }
                catch (Exception ex)
                {
                    return ex;
                }
            }, LazyThreadSafetyMode.ExecutionAndPublication);
        }

        public void Run(Action item)
        {
            Exception failure = null;
            try
            {
                Exception preparation = before.Value;
                if (preparation != null)
                {
                    throw new InvalidOperationException($"{name}: preparation failed: {preparation.Message}", preparation);
                }
                item();
            }
            catch (Exception ex)
            {
                failure = ex;
            }

            if (Interlocked.Decrement(ref remaining) == 0)
            {
                try
                {
                    after?.Invoke();
                }
                catch (Exception ex)
                {
                    failure ??= ex;
                }
            }

            if (failure != null)
            {
                ExceptionDispatchInfo.Capture(failure).Throw();
            }
        }
    }

    internal static class WorkQueue
    {
        /// <summary>
        /// Runs every item, biggest first, on up to <paramref name="maxTasks"/> threads (0 = unlimited). A failing item
        /// does not stop the others; the exceptions are thrown together as an AggregateException at the end.
        /// </summary>
        public static void RunLargestFirst<T>(IEnumerable<T> items, Func<T, long> size, int maxTasks, Action<T> run)
        {
            var ordered = Partitioner.Create(items.OrderByDescending(size), EnumerablePartitionerOptions.NoBuffering);
            var errors = new ConcurrentQueue<Exception>();
            Parallel.ForEach(ordered, Program.Parallelism(maxTasks), item =>
            {
                try
                {
                    run(item);
                }
                catch (Exception ex)
                {
                    errors.Enqueue(ex);
                }
            });
            if (!errors.IsEmpty)
            {
                throw new AggregateException(errors);
            }
        }
    }
}
