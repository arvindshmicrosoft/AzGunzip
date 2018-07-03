namespace Microsoft.Azure.Samples.AzGUnzip
{
    using CommandLine;

    [Verb("gunzip", HelpText = "Uncompresses GZip format files stored in Azure Storage; optionally 'chunking' the output into multiple blobs.")]
    class GUnzipOptions
    {
        [Option("SourceAccount", Required = true, HelpText = "Source storage account name")]
        public string SourceAccountName { get; set; }

        [Option("SourceContainer", Required = true, HelpText = "Source container name")]
        public string SourceContainer { get; set; }

        [Option("SourceKey", Required = false, HelpText = "Source storage account key")]
        public string SourceKey { get; set; }

        [Option("SourceSAS", Required = false, HelpText = "Source SAS")]
        public string SourceSAS { get; set; }

        [Option("SourceFile", Required = true, HelpText = "Source file name - must include folder name if applicable")]
        public string SourceFile { get; set; }

        [Option("DestAccount", Required = true, HelpText = "Destination storage account name")]
        public string DestAccountName { get; set; }

        [Option("DestContainer", Required = false, HelpText = "Destination container name")]
        public string DestContainer { get; set; }

        [Option("DestKey", Required = false, HelpText = "Destination storage account key")]
        public string DestKey { get; set; }

        [Option("DestSAS", Required = false, HelpText = "Destination SAS")]
        public string DestSAS { get; set; }

        [Option("DestBlobPrefix", Required = true, HelpText = "Prefix for the destination file names")]
        public string DestBlobPrefix { get; set; }

        [Option("DestBlobSuffix", Required = true, HelpText = "Suffix for the destination file names")]
        public string DestBlobSuffix { get; set; }

        [Option("DestBlobSize", Required = false, HelpText = "Destination file size threshold beyond which a new blob ('chunk') will be created")]
        public int DestBlobSize { get; set; }

        [Option("LineDelimiter", Required = false, HelpText = "Line delimiter (single character) which the 'chunking' process will use to correctly delineate chunks")]
        public string LineDelimiter { get; set; }
    }
}
