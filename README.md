
# AzGunzip
Uncompress (and optionally "chunk" the output) GZip archives in Azure Blob Storage

# Requirements
.NET Framework 4.6.1 is needed.

# Usage
Sample command line is shown below. This will take a file called somefile.gz from an Azure Storage account called someblobaccountname, under a container called somecontainerwithgzip and then chunk the output in 100MB chunks to another Azure storage account called someotheraccount under a container called someothercontainer, and for each chunk of output (== separate blob), prefix the blob name with "test_" and add a suffix called ".txt" to each blob's name. It uses \n to look for line endings and thereby make sure that the output chunks are "aligned" to respect line endings.

    AzGUnzip.exe --SourceAccount someblobaccountname --SourceContainer somecontainerwithgzip --SourceSAS "<<SAS here>>" --SourceFile somefile.gz --DestAccount someotheraccount --DestContainer someothercontainer --DestSAS "<<SAS here>>" --DestBlobPrefix "someprefix_" --DestBlobSuffix ".txt" --DestBlobSize 100000000 --LineDelimiter \n

# Acknowledgements
* Uses [SharpZipLib](github.com/icsharpcode/SharpZipLib) for uncompressing GZip files. Thanks to that team, especially so for fixing the Zip-slip vulnerability in the library just in time for this AzGunzip release!
* Uses [CommandLine](https://github.com/commandlineparser/commandline) for command-line parsing. Awesome library, I highly recommend this for anyone writing console apps!
