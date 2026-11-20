using Newtonsoft.Json;
using System.Globalization;
using Xunit;

namespace bcpJson.Tests
{
    public class ReadJsonValueTests
    {
        /// <summary>Positions a reader on the property name of {"v": jsonValue} and converts the value.</summary>
        private static object Read(string jsonValue, Type type)
        {
            using (var json = new JsonTextReader(new StringReader("{\"v\":" + jsonValue + "}")))
            {
                Assert.True(json.Read()); // StartObject
                Assert.True(json.Read()); // PropertyName
                Assert.Equal(JsonToken.PropertyName, json.TokenType);
                return Program.ReadJsonValue(json, type);
            }
        }

        [Theory]
        [InlineData(typeof(string))]
        [InlineData(typeof(object))]
        [InlineData(typeof(decimal))]
        [InlineData(typeof(double))]
        [InlineData(typeof(float))]
        [InlineData(typeof(int))]
        [InlineData(typeof(long))]
        [InlineData(typeof(short))]
        [InlineData(typeof(byte))]
        [InlineData(typeof(bool))]
        [InlineData(typeof(byte[]))]
        [InlineData(typeof(Guid))]
        [InlineData(typeof(DateTime))]
        [InlineData(typeof(DateTimeOffset))]
        [InlineData(typeof(TimeSpan))]
        public void Null_becomes_DBNull_for_every_type(Type type)
        {
            Assert.Same(DBNull.Value, Read("null", type));
        }

        [Fact]
        public void String_keeps_text_unchanged()
        {
            Assert.Equal("a \"quoted\" | piped\r\nline", Read("\"a \\\"quoted\\\" | piped\\r\\nline\"", typeof(string)));
        }

        [Fact]
        public void Decimal_keeps_full_precision()
        {
            Assert.Equal(12345678901234567890.1234567890m, Read("12345678901234567890.1234567890", typeof(decimal)));
            Assert.Equal(-0.0001m, Read("-0.0001", typeof(decimal)));
        }

        [Fact]
        public void Double_and_float()
        {
            Assert.Equal(1.5E+300, Read("1.5E+300", typeof(double)));
            Assert.Equal(3.25f, Read("3.25", typeof(float)));
        }

        [Fact]
        public void Integers_of_every_width()
        {
            Assert.Equal(int.MinValue, Read("-2147483648", typeof(int)));
            Assert.Equal(long.MaxValue, Read("9223372036854775807", typeof(long)));
            Assert.Equal((short)-32768, Read("-32768", typeof(short)));
            Assert.Equal((byte)255, Read("255", typeof(byte)));
        }

        [Fact]
        public void Bigint_written_as_string_also_parses()
        {
            Assert.Equal(long.MinValue, Read("\"-9223372036854775808\"", typeof(long)));
        }

        [Fact]
        public void Bool()
        {
            Assert.Equal(true, Read("true", typeof(bool)));
            Assert.Equal(false, Read("false", typeof(bool)));
        }

        [Fact]
        public void Binary_from_base64()
        {
            Assert.Equal(new byte[] { 0x00, 0xFF, 0x10, 0x7F }, Read("\"AP8Qfw==\"", typeof(byte[])));
        }

        [Fact]
        public void Guid_value()
        {
            var expected = Guid.Parse("6f9619ff-8b86-d011-b42d-00c04fc964ff");
            Assert.Equal(expected, Read("\"6F9619FF-8B86-D011-B42D-00C04FC964FF\"", typeof(Guid)));
        }

        [Fact]
        public void DateTime_keeps_ticks_and_unspecified_kind()
        {
            var value = (DateTime)Read("\"2024-02-29T23:59:59.1234567\"", typeof(DateTime));
            Assert.Equal(new DateTime(2024, 2, 29, 23, 59, 59, DateTimeKind.Unspecified).AddTicks(1234567), value);
            Assert.Equal(DateTimeKind.Unspecified, value.Kind);
        }

        [Fact]
        public void DateTimeOffset_keeps_its_offset()
        {
            var value = (DateTimeOffset)Read("\"2024-01-02T03:04:05.1234567+05:30\"", typeof(DateTimeOffset));
            Assert.Equal(TimeSpan.FromMinutes(330), value.Offset);
            Assert.Equal(new DateTime(2024, 1, 2, 3, 4, 5).AddTicks(1234567), value.DateTime);
        }

        [Fact]
        public void Time_value()
        {
            Assert.Equal(new TimeSpan(0, 12, 34, 56).Add(TimeSpan.FromTicks(1234567)), Read("\"12:34:56.1234567\"", typeof(TimeSpan)));
        }

        [Fact]
        public void Parsing_is_culture_invariant()
        {
            CultureInfo original = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = new CultureInfo("de-DE");
                Assert.Equal(3.25f, Read("\"3.25\"", typeof(float)));
                Assert.Equal(1.5m, Read("1.5", typeof(decimal)));
            }
            finally
            {
                CultureInfo.CurrentCulture = original;
            }
        }

        [Fact]
        public void Reader_is_left_on_the_value_so_the_next_property_can_be_read()
        {
            using (var json = new JsonTextReader(new StringReader("{\"a\":1,\"b\":\"x\"}")))
            {
                json.Read();
                json.Read();
                Assert.Equal(1, Program.ReadJsonValue(json, typeof(int)));
                Assert.True(json.Read());
                Assert.Equal("b", json.Value);
                Assert.Equal("x", Program.ReadJsonValue(json, typeof(string)));
            }
        }
    }
}
