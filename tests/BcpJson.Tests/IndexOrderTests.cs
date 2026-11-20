using Microsoft.SqlServer.Management.Smo;
using Xunit;

namespace bcpJson.Tests
{
    public class IndexOrderTests
    {
        [Theory]
        [InlineData(IndexType.ClusteredIndex, true, 0)]
        [InlineData(IndexType.ClusteredColumnStoreIndex, true, 0)]
        [InlineData(IndexType.NonClusteredIndex, false, 1)]
        [InlineData(IndexType.NonClusteredColumnStoreIndex, false, 1)]
        [InlineData(IndexType.PrimaryXmlIndex, false, 2)]
        [InlineData(IndexType.SecondaryXmlIndex, false, 3)]
        [InlineData(IndexType.SpatialIndex, false, 4)]
        public void Rank(IndexType type, bool isClustered, int expected)
        {
            Assert.Equal(expected, Program.IndexCreationRank(type, isClustered));
        }

        [Fact]
        public void Clustered_flag_wins_over_type()
        {
            Assert.Equal(0, Program.IndexCreationRank(IndexType.NonClusteredIndex, isClustered: true));
        }

        [Fact]
        public void Dependencies_come_first()
        {
            // The clustered primary key before everything; primary XML before secondary XML; spatial after the PK.
            Assert.True(Program.IndexCreationRank(IndexType.ClusteredIndex, true) < Program.IndexCreationRank(IndexType.NonClusteredIndex, false));
            Assert.True(Program.IndexCreationRank(IndexType.PrimaryXmlIndex, false) < Program.IndexCreationRank(IndexType.SecondaryXmlIndex, false));
            Assert.True(Program.IndexCreationRank(IndexType.ClusteredIndex, true) < Program.IndexCreationRank(IndexType.SpatialIndex, false));
        }
    }
}
