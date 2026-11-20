using Microsoft.Data.SqlClient;
using Xunit;

namespace bcpJson.Tests
{
    public class SqlConnectionFactoryTests
    {
        private static SqlConnectionStringBuilder Build(string server, string database, string login, string password, bool readOnly = false, bool trust = false)
        {
            return new SqlConnectionStringBuilder(SqlConnectionFactory.Build(server, database, login, password, readOnly, trust));
        }

        [Fact]
        public void Defaults_to_localhost_master_and_windows_auth()
        {
            var b = Build(null, "", "", null);
            Assert.Equal("localhost", b.DataSource);
            Assert.Equal("master", b.InitialCatalog);
            Assert.True(b.IntegratedSecurity);
            Assert.Equal("", b.UserID);
            Assert.Equal("bcpJson", b.ApplicationName);
            Assert.False(b.TrustServerCertificate);
        }

        [Fact]
        public void Sql_login_disables_integrated_security()
        {
            var b = Build(@"srv\inst,1433", "Sales", "loader", "pw");
            Assert.Equal(@"srv\inst,1433", b.DataSource);
            Assert.Equal("Sales", b.InitialCatalog);
            Assert.False(b.IntegratedSecurity);
            Assert.Equal("loader", b.UserID);
            Assert.Equal("pw", b.Password);
        }

        [Theory]
        [InlineData("p;w=d")]
        [InlineData("it's \"quoted\"")]
        [InlineData(" spaced ")]
        public void Special_characters_in_password_and_database_round_trip(string value)
        {
            var b = Build("srv", value, "user", value);
            Assert.Equal(value, b.Password);
            Assert.Equal(value, b.InitialCatalog);
        }

        [Fact]
        public void Login_without_password_uses_empty_password()
        {
            var b = Build("srv", "db", "user", null);
            Assert.Equal("", b.Password);
            Assert.False(b.IntegratedSecurity);
        }

        [Fact]
        public void ReadOnly_and_trust_flags()
        {
            var b = Build("srv", "db", "", "", readOnly: true, trust: true);
            Assert.Equal(ApplicationIntent.ReadOnly, b.ApplicationIntent);
            Assert.True(b.TrustServerCertificate);

            Assert.Equal(ApplicationIntent.ReadWrite, Build("srv", "db", "", "").ApplicationIntent);
        }
    }
}
