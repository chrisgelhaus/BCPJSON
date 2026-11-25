using Xunit;

namespace bcpJson.Tests
{
    public class DocsTests
    {
        [Theory]
        [InlineData("Install", "install")]
        [InlineData("Load as a heap: `--load-as-heap`", "load-as-a-heap---load-as-heap")]
        [InlineData("Keys and indexes", "keys-and-indexes")]
        [InlineData("JSON format", "json-format")]
        public void Slug(string heading, string expected)
        {
            Assert.Contains(expected, DocsFiles.Anchors("## " + heading + "\n"));
        }

        [Fact]
        public void Repeated_headings_get_numbered_slugs()
        {
            Assert.Equal(new[] { "copy", "copy-1" }, DocsFiles.Anchors("## Copy\n### Copy\n").OrderBy(x => x, StringComparer.Ordinal));
        }

        [Fact]
        public void Headings_in_code_blocks_are_not_anchors()
        {
            Assert.Empty(DocsFiles.Anchors("```\n# not a heading\n```\n"));
        }

        [Fact]
        public void Links_outside_code_only()
        {
            var links = DocsFiles.RelativeLinks("[a](x.md#b) `[c](y.md)` [d](https://e) [f](#g)\n```\n[h](z.md)\n```\n").ToList();
            Assert.Equal(new[] { ("x.md", "b"), ("", "g") }, links);
        }

        [Fact]
        public void Options_in_code_spans_and_blocks()
        {
            var options = DocsFiles.CodeSpanOptions("Use `--snapshot` and --not-this.\n```\nbcpjson export --source-database Sales\n```\n").ToList();
            Assert.Equal(new[] { "--snapshot", "--source-database" }, options);
        }

        [Fact]
        public void Options_of_other_commands_in_code_blocks_are_ignored()
        {
            var options = DocsFiles.CodeSpanOptions("```\ngit diff --no-index a b\nbcpjson export --source-database Sales `\n    --snapshot\nsqlcmd -S x --other\n```\n").ToList();
            Assert.Equal(new[] { "--source-database", "--snapshot" }, options);
        }

        [Fact]
        public void Relative_links_and_anchors_resolve()
        {
            var failures = new List<string>();
            foreach (string file in DocsFiles.DocFiles())
            {
                string folder = Path.GetDirectoryName(file);
                foreach ((string target, string anchor) in DocsFiles.RelativeLinks(File.ReadAllText(file)))
                {
                    string path = target.Length == 0 ? file : Path.GetFullPath(Path.Combine(folder, Uri.UnescapeDataString(target)));
                    string where = Path.GetRelativePath(DocsFiles.RepoRoot(), file) + ": (" + target + (anchor == null ? "" : "#" + anchor) + ")";
                    if (!File.Exists(path) && !Directory.Exists(path))
                    {
                        failures.Add(where + " missing");
                    }
                    else if (anchor != null && path.EndsWith(".md", StringComparison.OrdinalIgnoreCase)
                        && !DocsFiles.Anchors(File.ReadAllText(path)).Contains(anchor))
                    {
                        failures.Add(where + " no such heading");
                    }
                }
            }
            Assert.True(failures.Count == 0, "Broken links:\n" + string.Join("\n", failures));
        }

        [Fact]
        public void User_docs_link_only_to_shipped_files()
        {
            string root = DocsFiles.RepoRoot();
            string guides = Path.Combine(root, "docs", "guides") + Path.DirectorySeparatorChar;
            string reference = Path.Combine(root, "docs", "reference") + Path.DirectorySeparatorChar;
            var shipped = new HashSet<string>(new[] { "LICENSE", "NOTICE", "README.md" }.Select(n => Path.Combine(root, n)), StringComparer.OrdinalIgnoreCase);
            var offenders = new List<string>();
            foreach (string file in DocsFiles.DocFiles().Where(f => !f.EndsWith("CONTRIBUTING.md", StringComparison.OrdinalIgnoreCase)))
            {
                foreach ((string target, string _) in DocsFiles.RelativeLinks(File.ReadAllText(file)).Where(l => l.Target.Length > 0))
                {
                    string path = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(file), Uri.UnescapeDataString(target)));
                    if (!shipped.Contains(path) && !path.StartsWith(guides, StringComparison.OrdinalIgnoreCase)
                        && !path.StartsWith(reference, StringComparison.OrdinalIgnoreCase))
                    {
                        offenders.Add(Path.GetRelativePath(root, file) + ": " + target);
                    }
                }
            }
            Assert.True(offenders.Count == 0, "Links to files that are not in the release package (use a GitHub URL):\n" + string.Join("\n", offenders));
        }

        public static TheoryData<Type, string> Verbs => new()
        {
            { typeof(ExportOptions), "export.md" },
            { typeof(ImportOptions), "import.md" },
            { typeof(CopyOptions), "copy.md" },
            { typeof(VerifyOptions), "verify.md" },
        };

        private static IEnumerable<CommandLine.OptionAttribute> OptionsOf(Type options)
        {
            return options.GetProperties()
                .Select(p => p.GetCustomAttributes(typeof(CommandLine.OptionAttribute), false).Cast<CommandLine.OptionAttribute>().FirstOrDefault())
                .Where(a => a != null);
        }

        [Theory, MemberData(nameof(Verbs))]
        public void Every_option_is_documented_on_its_verb_page(Type options, string page)
        {
            string path = Path.Combine(DocsFiles.RepoRoot(), "docs", "reference", page);
            string text = File.Exists(path) ? File.ReadAllText(path) : "";
            var missing = OptionsOf(options)
                .Select(a => string.IsNullOrEmpty(a.LongName) ? "-" + a.ShortName : "--" + a.LongName)
                .Where(name => !text.Contains("`" + name + "`", StringComparison.Ordinal))
                .ToList();
            Assert.True(missing.Count == 0, page + " does not document: " + string.Join(", ", missing));
        }

        [Fact]
        public void Every_documented_option_exists()
        {
            var known = new HashSet<string>(StringComparer.Ordinal) { "--help", "--version" };
            foreach (Type options in new[] { typeof(ExportOptions), typeof(ImportOptions), typeof(CopyOptions), typeof(VerifyOptions) })
            {
                foreach (var a in OptionsOf(options).Where(a => !string.IsNullOrEmpty(a.LongName)))
                {
                    known.Add("--" + a.LongName);
                }
            }
            string root = DocsFiles.RepoRoot();
            var unknown = new List<string>();
            foreach (string folder in new[] { "guides", "reference" })
            {
                string path = Path.Combine(root, "docs", folder);
                foreach (string file in Directory.Exists(path) ? Directory.GetFiles(path, "*.md") : Array.Empty<string>())
                {
                    unknown.AddRange(DocsFiles.CodeSpanOptions(File.ReadAllText(file)).Where(o => !known.Contains(o))
                        .Distinct().Select(o => Path.GetFileName(file) + ": " + o));
                }
            }
            Assert.Empty(unknown);
        }
    }
}

namespace bcpJson.Tests
{
    /// <summary>Reads the user docs: files, relative links, GitHub heading anchors and options named in code.</summary>
    internal static class DocsFiles
    {
        private static readonly System.Text.RegularExpressions.Regex Link =
            new(@"!?\[[^\]]*\]\((?<url>[^)\s]+)(?:\s+""[^""]*"")?\)");
        private static readonly System.Text.RegularExpressions.Regex InlineCode = new(@"`[^`\n]*`");
        private static readonly System.Text.RegularExpressions.Regex Heading = new(@"^#{1,6}\s+(?<text>.*?)\s*#*\s*$");
        private static readonly System.Text.RegularExpressions.Regex OptionName = new(@"--[a-z][a-z0-9-]*");

        internal static string RepoRoot()
        {
            for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
            {
                if (File.Exists(Path.Combine(dir.FullName, "BcpJson.sln")))
                {
                    return dir.FullName;
                }
            }
            throw new InvalidOperationException("BcpJson.sln not found above " + AppContext.BaseDirectory);
        }

        internal static IEnumerable<string> DocFiles()
        {
            string root = RepoRoot();
            foreach (string name in new[] { "README.md", "CONTRIBUTING.md" })
            {
                string path = Path.Combine(root, name);
                if (File.Exists(path))
                {
                    yield return path;
                }
            }
            foreach (string folder in new[] { "guides", "reference" })
            {
                string path = Path.Combine(root, "docs", folder);
                if (Directory.Exists(path))
                {
                    foreach (string file in Directory.GetFiles(path, "*.md").OrderBy(f => f, StringComparer.Ordinal))
                    {
                        yield return file;
                    }
                }
            }
        }

        /// <summary>Lines of the document with a flag saying whether each is inside a fenced code block (fence lines count as inside).</summary>
        private static IEnumerable<(string Line, bool InFence)> Lines(string markdown)
        {
            bool inFence = false;
            foreach (string raw in markdown.Replace("\r\n", "\n").Split('\n'))
            {
                string trimmed = raw.TrimStart();
                if (trimmed.StartsWith("```", StringComparison.Ordinal) || trimmed.StartsWith("~~~", StringComparison.Ordinal))
                {
                    inFence = !inFence;
                    yield return (raw, true);
                    continue;
                }
                yield return (raw, inFence);
            }
        }

        internal static IEnumerable<(string Target, string Anchor)> RelativeLinks(string markdown)
        {
            foreach ((string line, bool inFence) in Lines(markdown))
            {
                if (inFence)
                {
                    continue;
                }
                foreach (System.Text.RegularExpressions.Match m in Link.Matches(InlineCode.Replace(line, "")))
                {
                    string url = m.Groups["url"].Value;
                    if (url.StartsWith("http:", StringComparison.OrdinalIgnoreCase) || url.StartsWith("https:", StringComparison.OrdinalIgnoreCase)
                        || url.StartsWith("mailto:", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }
                    int hash = url.IndexOf('#');
                    yield return hash < 0 ? (url, null) : (url.Substring(0, hash), url.Substring(hash + 1));
                }
            }
        }

        internal static IReadOnlySet<string> Anchors(string markdown)
        {
            var anchors = new HashSet<string>(StringComparer.Ordinal);
            var seen = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach ((string line, bool inFence) in Lines(markdown))
            {
                var m = Heading.Match(line);
                if (inFence || !m.Success)
                {
                    continue;
                }
                string slug = new string(m.Groups["text"].Value.ToLowerInvariant()
                    .Where(c => char.IsLetterOrDigit(c) || c == ' ' || c == '-' || c == '_').ToArray()).Replace(' ', '-');
                if (seen.TryGetValue(slug, out int count))
                {
                    seen[slug] = count + 1;
                    anchors.Add(slug + "-" + (count + 1).ToString(System.Globalization.CultureInfo.InvariantCulture));
                }
                else
                {
                    seen[slug] = 0;
                    anchors.Add(slug);
                }
            }
            return anchors;
        }

        /// <summary>
        /// Every --name inside inline code spans, and inside fenced code blocks on bcpjson command lines only (a line starting
        /// with "bcpjson", and its continuation lines after a trailing ` or \); other commands' flags (git, sqlcmd) are not ours.
        /// </summary>
        internal static IEnumerable<string> CodeSpanOptions(string markdown)
        {
            bool inCommand = false;
            foreach ((string line, bool inFence) in Lines(markdown))
            {
                if (inFence)
                {
                    string trimmed = line.Trim();
                    bool continued = inCommand;
                    inCommand = trimmed.StartsWith("bcpjson ", StringComparison.Ordinal) || (continued && trimmed.Length > 0);
                    bool collect = inCommand;
                    inCommand = inCommand && (trimmed.EndsWith('`') || trimmed.EndsWith('\\'));
                    if (!collect)
                    {
                        continue;
                    }
                }
                IEnumerable<string> code = inFence ? new[] { line } : InlineCode.Matches(line).Select(m => m.Value);
                foreach (string part in code)
                {
                    foreach (System.Text.RegularExpressions.Match m in OptionName.Matches(part))
                    {
                        yield return m.Value;
                    }
                }
            }
        }
    }
}
