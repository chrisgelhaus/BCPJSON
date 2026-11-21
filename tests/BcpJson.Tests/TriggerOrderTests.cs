using Microsoft.SqlServer.Management.Smo.Agent;
using Xunit;

namespace bcpJson.Tests
{
    public class TriggerOrderTests
    {
        [Fact]
        public void No_statements_when_order_is_not_set()
        {
            Assert.Empty(Program.TriggerOrderStatements("[dbo].[TR]", ActivationOrder.None, ActivationOrder.None, ActivationOrder.None));
        }

        [Fact]
        public void One_statement_per_statement_type_with_first_or_last()
        {
            var statements = Program.TriggerOrderStatements("[dbo].[TR]", ActivationOrder.First, ActivationOrder.None, ActivationOrder.Last).ToList();
            Assert.Equal(new[]
            {
                "EXEC sp_settriggerorder @triggername = N'[dbo].[TR]', @order = N'First', @stmttype = N'INSERT';",
                "EXEC sp_settriggerorder @triggername = N'[dbo].[TR]', @order = N'Last', @stmttype = N'DELETE';"
            }, statements);
        }

        [Fact]
        public void Trigger_name_is_a_safe_literal()
        {
            string statement = Assert.Single(Program.TriggerOrderStatements("[my sch].[it's]]]", ActivationOrder.None, ActivationOrder.First, ActivationOrder.None));
            Assert.Contains("@triggername = N'[my sch].[it''s]]]'", statement);
        }
    }
}
