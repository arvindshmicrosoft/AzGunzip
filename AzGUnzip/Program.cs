namespace Microsoft.Azure.Samples.AzGUnzip
{
    using CommandLine;
    using Engine;
    using System.Linq;
    using System.Text.RegularExpressions;

    class Program
    {
        static void Main(string[] args)
        {
            var parseResult = CommandLine.Parser.Default.ParseArguments<GUnzipOptions>(args)
                .MapResult(
                (GUnzipOptions opts) => {
                    return GZipExtractor.GUnzipAndChunk(
                        opts.SourceAccountName,
                        opts.SourceContainer,
                        opts.SourceKey,
                        opts.SourceSAS,
                        opts.SourceFile,
                        opts.DestAccountName,
                        opts.DestKey,
                        opts.DestSAS,
                        opts.DestContainer,
                        opts.DestBlobPrefix,
                        opts.DestBlobSuffix,
                        opts.DestBlobSize,
                        Regex.Unescape(opts.LineDelimiter)[0]) ? 0 : 1;
                },
                errs => 1);
        }
    }
}
