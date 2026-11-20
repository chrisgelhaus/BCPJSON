using Microsoft.Data.SqlClient;
using Xunit;

namespace bcpJson.Tests
{
    public class SqlBulkCopyHelperTests
    {
        /// <summary>
        /// SqlBulkCopyHelper finds the rows-copied counter by reflection because its name has changed across
        /// SqlClient versions. If an upgrade renames it again the helper returns -1 and copy would report wrong
        /// row counts while still moving the data correctly; this test turns that into a build failure.
        /// </summary>
        [Fact]
        public void Finds_the_rows_copied_counter_in_this_SqlClient_version()
        {
            // No connection is opened: constructing SqlBulkCopy only parses the connection string.
            using (var bulkCopy = new SqlBulkCopy("Server=localhost;Database=master;Integrated Security=true"))
            {
                Assert.Equal(0, SqlBulkCopyHelper.GetRowsCopied(bulkCopy));
            }
        }
    }
}
