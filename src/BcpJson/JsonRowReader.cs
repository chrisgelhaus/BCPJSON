using Newtonsoft.Json;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;

namespace bcpJson
{
    /// <summary>
    /// Presents an exported JSON file (an array of row objects, one per line) as a forward-only data reader, so
    /// SqlBulkCopy pulls rows straight from the file: memory stays at one row however large the table, and parsing
    /// overlaps with sending. Values are converted to the destination column types with <see cref="Program.ReadJsonValue"/>
    /// (decimal columns as SqlDecimal, which SqlBulkCopy passes on with all 38 digits). Properties the target does not
    /// have are skipped; columns missing from a row are NULL.
    /// </summary>
    internal sealed class JsonRowReader : DbDataReader
    {
        private readonly JsonTextReader json;
        private readonly string[] names;
        private readonly Type[] types;
        private readonly Dictionary<string, int> ordinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        private readonly object[] values;
        private bool done;
        private bool closed;

        public JsonRowReader(TextReader text, IReadOnlyList<string> names, IReadOnlyList<Type> types)
        {
            this.names = names.ToArray();
            this.types = types.ToArray();
            this.values = new object[this.names.Length];
            for (int i = 0; i < this.names.Length; i++)
            {
                this.ordinals[this.names[i]] = i;
            }

            this.json = new JsonTextReader(text)
            {
                DateParseHandling = DateParseHandling.None,
                FloatParseHandling = FloatParseHandling.Decimal,
                CloseInput = false
            };
            if (!this.json.Read() || this.json.TokenType != JsonToken.StartArray)
            {
                throw new InvalidDataException("Expected a JSON array of row objects.");
            }
        }

        /// <summary>Rows read so far.</summary>
        public long RowCount { get; private set; }

        public override bool Read()
        {
            if (this.done)
            {
                return false;
            }
            if (!this.json.Read())
            {
                throw new InvalidDataException("The JSON file ends before the closing ']'.");
            }
            if (this.json.TokenType == JsonToken.EndArray)
            {
                this.done = true;
                return false;
            }
            if (this.json.TokenType != JsonToken.StartObject)
            {
                throw new InvalidDataException(string.Format("Expected a row object at line {0}.", this.json.LineNumber));
            }

            Array.Fill(this.values, DBNull.Value);
            while (this.json.Read() && this.json.TokenType != JsonToken.EndObject)
            {
                string property = (string)this.json.Value;
                if (this.ordinals.TryGetValue(property, out int ordinal))
                {
                    try
                    {
                        this.values[ordinal] = Program.ReadJsonValue(this.json, this.types[ordinal]);
                    }
                    catch (Exception ex) when (ex is FormatException || ex is OverflowException || ex is JsonReaderException || ex is InvalidCastException)
                    {
                        throw new InvalidDataException(string.Format("Line {0}, column {1}: {2}", this.json.LineNumber, property, ex.Message), ex);
                    }
                }
                else
                {
                    this.json.Read();
                    this.json.Skip();
                }
            }
            if (this.json.TokenType != JsonToken.EndObject)
            {
                throw new InvalidDataException(string.Format("Row at line {0} is not closed.", this.json.LineNumber));
            }

            this.RowCount++;
            return true;
        }

        public override int FieldCount => this.names.Length;
        public override string GetName(int ordinal) => this.names[ordinal];
        public override Type GetFieldType(int ordinal) => this.types[ordinal];
        public override string GetDataTypeName(int ordinal) => this.types[ordinal].Name;

        public override int GetOrdinal(string name)
        {
            if (this.ordinals.TryGetValue(name, out int ordinal))
            {
                return ordinal;
            }
            throw new IndexOutOfRangeException(name);
        }

        public override object GetValue(int ordinal) => this.values[ordinal];
        public override bool IsDBNull(int ordinal) => this.values[ordinal] is DBNull;
        public override object this[int ordinal] => this.GetValue(ordinal);
        public override object this[string name] => this.GetValue(this.GetOrdinal(name));

        public override int GetValues(object[] target)
        {
            int count = Math.Min(target.Length, this.values.Length);
            Array.Copy(this.values, target, count);
            return count;
        }

        public override bool GetBoolean(int ordinal) => (bool)this.values[ordinal];
        public override byte GetByte(int ordinal) => (byte)this.values[ordinal];
        public override char GetChar(int ordinal) => ((string)this.values[ordinal])[0];
        public override DateTime GetDateTime(int ordinal) => (DateTime)this.values[ordinal];
        public override decimal GetDecimal(int ordinal) => (decimal)this.values[ordinal];
        public override double GetDouble(int ordinal) => (double)this.values[ordinal];
        public override float GetFloat(int ordinal) => (float)this.values[ordinal];
        public override Guid GetGuid(int ordinal) => (Guid)this.values[ordinal];
        public override short GetInt16(int ordinal) => (short)this.values[ordinal];
        public override int GetInt32(int ordinal) => (int)this.values[ordinal];
        public override long GetInt64(int ordinal) => (long)this.values[ordinal];
        public override string GetString(int ordinal) => (string)this.values[ordinal];

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return CopyRange((byte[])this.values[ordinal], dataOffset, buffer, bufferOffset, length);
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return CopyRange(((string)this.values[ordinal]).ToCharArray(), dataOffset, buffer, bufferOffset, length);
        }

        private static long CopyRange<T>(T[] source, long offset, T[] buffer, int bufferOffset, int length)
        {
            if (buffer == null)
            {
                return source.Length;
            }
            int count = (int)Math.Max(0, Math.Min(length, source.Length - offset));
            Array.Copy(source, offset, buffer, bufferOffset, count);
            return count;
        }

        public override bool HasRows => true;
        public override bool IsClosed => this.closed;
        public override int RecordsAffected => -1;
        public override int Depth => 0;
        public override bool NextResult() => false;
        public override IEnumerator GetEnumerator() => new DbEnumerator(this);

        public override void Close()
        {
            this.closed = true;
            this.json.Close();
        }
    }
}
