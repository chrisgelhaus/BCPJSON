using Newtonsoft.Json;
using System.Data.SqlTypes;
using System.Globalization;
using Xunit;

namespace bcpJson.Tests
{
    /// <summary>decimal(38, s) values beyond System.Decimal's 28-29 significant digits must survive export and import.</summary>
    public class DecimalPrecisionTests
    {
        private const string MaxDecimal38 = "99999999999999999999999999999999999999";
        private const string TinyDecimal38 = "0.00000000000000000000000000000000000001";

        private static string Write(SqlDecimal value)
        {
            var text = new StringWriter();
            using (var writer = new JsonTextWriter(text))
            {
                writer.WriteStartArray();
                Program.WriteJsonDecimal(writer, value);
                writer.WriteEndArray();
            }
            string json = text.ToString();
            return json.Substring(1, json.Length - 2);
        }

        private static object Read(string jsonValue)
        {
            using (var json = new JsonTextReader(new StringReader("{\"v\":" + jsonValue + "}")))
            {
                json.FloatParseHandling = FloatParseHandling.Decimal; // as JSON import sets it
                Assert.True(json.Read());
                Assert.True(json.Read());
                return Program.ReadJsonValue(json, typeof(SqlDecimal));
            }
        }

        [Theory]
        [InlineData(MaxDecimal38)]
        [InlineData("-" + MaxDecimal38)]
        [InlineData(TinyDecimal38)]
        [InlineData("1234567890123456789012345678.0123456789")]
        [InlineData("-0.1234567890123456789012345678901234567")]
        public void Values_beyond_System_Decimal_round_trip_exactly(string text)
        {
            SqlDecimal value = SqlDecimal.Parse(text);
            Assert.False(Program.FitsSystemDecimal(value));

            string json = Write(value);
            Assert.Equal(text, json);

            var read = Assert.IsType<SqlDecimal>(Read(json));
            Assert.Equal(text, read.ToString());
        }

        [Theory]
        [InlineData("12345.6789012345")]
        [InlineData("123456789012345678.0123456789")]
        [InlineData("-0.0000000001")]
        [InlineData("7922816251426433759354395033")]
        public void Values_that_fit_System_Decimal_are_written_as_before(string text)
        {
            SqlDecimal value = SqlDecimal.Parse(text);
            Assert.True(Program.FitsSystemDecimal(value));

            var expected = new StringWriter();
            using (var writer = new JsonTextWriter(expected))
            {
                writer.WriteStartArray();
                writer.WriteValue(value.Value);
                writer.WriteEndArray();
            }
            Assert.Equal(expected.ToString(), "[" + Write(value) + "]");

            Assert.Equal(value.Value, ((SqlDecimal)Read(Write(value))).Value);
        }

        [Fact]
        public void Null_quoted_and_exponent_values_are_read()
        {
            Assert.Same(DBNull.Value, Read("null"));
            Assert.Equal(MaxDecimal38, ((SqlDecimal)Read("\"" + MaxDecimal38 + "\"")).ToString());
            Assert.Equal(1500m, ((SqlDecimal)Read("1.5E3")).Value);
        }

        [Fact]
        public void Text_ignores_the_current_culture()
        {
            CultureInfo saved = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = new CultureInfo("de-DE");
                string text = "-1234567890123456789012345678.0123456789";
                Assert.Equal(text, Write(SqlDecimal.Parse(text)));
                Assert.Equal(text, ((SqlDecimal)Read(text)).ToString());
            }
            finally
            {
                CultureInfo.CurrentCulture = saved;
            }
        }
    }
}
