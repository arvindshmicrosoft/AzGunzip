using ICSharpCode.SharpZipLib.GZip;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Engine
{
    public class GZipExtractor
    {
        private static void GlobalOptimizations()
        {
            // First two are Best practices optimizations for Blob, as per the Azure Storage GitHub these are highly recommended.
            // The Threadpool setting - I had just played with it. Leaving it in here given many of the operations are effectively still sync
            ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount * 8;
            ServicePointManager.Expect100Continue = false;
            ThreadPool.SetMinThreads(Environment.ProcessorCount * 25, Environment.ProcessorCount * 8);
        }

        public static bool GUnzipAndChunk(
            string sourceStorageAccountName,
            string sourceStorageContainerName,
            string sourceStorageAccountKey,
            string sourceSAS,
            string sourceBlobName,
            string destStorageAccountName,
            string destStorageAccountKey,
            string destSAS,
            string destStorageContainerName,
            string destBlobPrefix,
            string destBlobSuffix,
            int destBlobSizeLimitBytes,
            char lineDelimiter
            )
        {
            GlobalOptimizations();

            var typeOfSourceCredential = string.IsNullOrEmpty(sourceSAS) ? "AccountKey" : "SharedAccessSignature";
            var sourceCredential = string.IsNullOrEmpty(sourceSAS) ? sourceStorageAccountKey : sourceSAS;

            var typeOfDestCredential = string.IsNullOrEmpty(destSAS) ? "AccountKey" : "SharedAccessSignature";
            var destCredential = string.IsNullOrEmpty(destSAS) ? destStorageAccountKey : destSAS;

            // TODO remove hard-coding of core.windows.net
            string sourceAzureStorageConnStr = $"DefaultEndpointsProtocol=https;AccountName={sourceStorageAccountName};{typeOfSourceCredential}={sourceCredential};EndpointSuffix=core.windows.net";
            string destAzureStorageConnStr = $"DefaultEndpointsProtocol=https;AccountName={destStorageAccountName};{typeOfDestCredential}={destCredential};EndpointSuffix=core.windows.net";

            var sourceStorageAccount = CloudStorageAccount.Parse(sourceAzureStorageConnStr);
            var sourceBlobClient = sourceStorageAccount.CreateCloudBlobClient();
            var sourceContainer = sourceBlobClient.GetContainerReference(sourceStorageContainerName);
            var sourceBlob = sourceContainer.GetBlockBlobReference(sourceBlobName);

            var destStorageAccount = CloudStorageAccount.Parse(destAzureStorageConnStr);
            var destBlobClient = destStorageAccount.CreateCloudBlobClient();
            var destContainer = destBlobClient.GetContainerReference(destStorageContainerName);

            // We need to consider the lesser of 2 values for the output buffer size:
            // 100 MB which is the upper limit per block for the current Azure Block Blob PutBlock API
            // or destBlobSizeLimitBytes, which is the ideal size per output chunk blob
            int OUTPUT_BUFFER_SIZE = Math.Min(100 * 1024 * 1024, destBlobSizeLimitBytes);

            int sleepDuration = 1000;

            // this outer loop is for retrying if the source blob read operations were throttled
            // in such a case we will basically start from scratch as with the uncompress operation there is 
            // a challenge around where exactly in the source stream to resume from
            // TODO implement some form of journal / checkpoint to better support resume operations
            while (true)
            {
                byte[] outputmemBuff = new byte[OUTPUT_BUFFER_SIZE];

                long totalBytesUncompressed = 0;
                long bytesUncompressedToCurrentBlob = 0;

                int outputFileIndex = 0;
                var destBlobName = $"{destBlobPrefix}{outputFileIndex:000000}{destBlobSuffix}";
                var destBlob = destContainer.GetBlockBlobReference(destBlobName);

                var finalBlockList = new List<string>();

                try
                {
                    using (var blobStream = sourceBlob.OpenRead())
                    {
                        using (var csStream = new GZipInputStream(blobStream))
                        {
                            int nRead = 0;
                            int currOutputBuffOffset = 0;
                            int blockIndex = 0;

                            var taskList = new List<Task<string>>();

                            while (true)
                            {
                                nRead = csStream.Read(outputmemBuff, currOutputBuffOffset, OUTPUT_BUFFER_SIZE - currOutputBuffOffset);
                                totalBytesUncompressed += nRead;
                                bytesUncompressedToCurrentBlob += nRead;

                                // have we filled up the output buffer, or are we done uncompressing?
                                if (currOutputBuffOffset + nRead == OUTPUT_BUFFER_SIZE || nRead == 0)
                                {
                                    if (currOutputBuffOffset + nRead > 0)
                                    {
                                        // work backwards from the "right" side of the buffer and check for the lineDelimiter
                                        // and then we treat that and whatever is to the left as candidate for the next blob block
                                        // and spool the reminder to the next iteration

                                        var lastDelimOffset = FindLastInstanceOfDelimiter(outputmemBuff, lineDelimiter);

                                        // save a reference to the previous buffer
                                        var prevBuff = outputmemBuff;

                                        // create a new buffer for the next iteration
                                        outputmemBuff = new byte[OUTPUT_BUFFER_SIZE];

                                        // marker for the new buffer is 
                                        currOutputBuffOffset = OUTPUT_BUFFER_SIZE - 1 - lastDelimOffset;

                                        // copy the trailing portion of previous array beyond last delimiter into the new array
                                        Array.Copy(prevBuff, lastDelimOffset + 1, outputmemBuff, 0, currOutputBuffOffset);

                                        // finally save the previous buffer, up to the last delimiter
                                        //using (var fd = File.Create($@"C:\Users\arvindsh\AppData\Local\Temp\test_{outputFileIndex:0000}.txt"))
                                        //{
                                        //    fd.Write(prevBuff, 0, lastDelimOffset + 1);
                                        //    fd.Flush();
                                        //}

                                        // save chunk to blob asynchronously; this method will return a blockId
                                        taskList.Add(Task<string>.Factory.StartNew(() => CreateBlockforBlob(destBlob,
                                            blockIndex,
                                            prevBuff,
                                            lastDelimOffset + 1
                                            )
                                            ));

                                        blockIndex++;

                                        // check the per-blob size limit and if so put the block list and move on to the next block blob
                                        // there are multiple ways to do this check: 
                                        // * one is to check if the current byte count written to the current blob is at (let's say) 
                                        //   95% threshold of the destination blob size limit.
                                        // * the other way is to check if the sum of bytes written to current blob + output buffer size 
                                        //   would go over the dest blob size limit, and if so put the block list
                                        // we chose the latter as it seems more deterministic and not based on some % value which would be debatable
                                        if (bytesUncompressedToCurrentBlob + OUTPUT_BUFFER_SIZE >= destBlobSizeLimitBytes || nRead == 0)
                                        {
                                            // first we need to make sure all the tasks which are writing to individual blocks
                                            // complete and then gather the final block list
                                            Task.WaitAll(taskList.ToArray());

                                            finalBlockList = (from t in taskList
                                                              select t.Result).ToList();

                                            PubBlockList(destBlob, finalBlockList);

                                            // reset the task list now since we are going on to the next blob
                                            taskList.Clear();

                                            // reset bytes written to current blob
                                            bytesUncompressedToCurrentBlob = 0;

                                            // increment the output blob "index"
                                            outputFileIndex++;

                                            // create the next blob
                                            destBlobName = $"{destBlobPrefix}{outputFileIndex:000000}{destBlobSuffix}";
                                            destBlob = destContainer.GetBlockBlobReference(destBlobName);

                                            // reset the block details for the new blob
                                            blockIndex = 0;
                                            finalBlockList = new List<string>();
                                        }

                                        // prevBuff = null;
                                    }
                                }
                                else
                                {
                                    currOutputBuffOffset += nRead;
                                }

                                if (nRead == 0)
                                {
                                    break;
                                }
                            }
                        }
                    }
                }
                catch (StorageException ex)
                {
                    Debug.WriteLine($"Error received: {ex.Message} with Error code {ex.RequestInformation.ErrorCode}");

                    foreach (var addDetails in ex.RequestInformation.ExtendedErrorInformation.AdditionalDetails)
                    {
                        Debug.WriteLine($"{addDetails.Key}: {addDetails.Value}");
                    }

                    if ("ServerBusy" == ex.RequestInformation.ErrorCode || "InternalError" == ex.RequestInformation.ErrorCode || "OperationTimedOut" == ex.RequestInformation.ErrorCode)
                    {
                        // TODO reset the Blob stream

                        System.Threading.Thread.Sleep(sleepDuration);
                        sleepDuration *= 2;
                        continue;
                    }
                    else
                    {
                        throw;
                    }
                }

                break;
            }

            return true;
        }

        private static void PubBlockList(CloudBlockBlob destBlob, List<string> finalBlockList)
        {
            int sleepDuration = 1000;

            while (true)
            {
                try
                {
                    destBlob.PutBlockListAsync(finalBlockList).GetAwaiter().GetResult();
                }
                catch (StorageException ex)
                {
                    Debug.WriteLine($"Error received during PutBlockList: {ex.Message}");

                    foreach (var addDetails in ex.RequestInformation.ExtendedErrorInformation.AdditionalDetails)
                    {
                        Debug.WriteLine($"{addDetails.Key}: {addDetails.Value}");
                    }

                    if ("ServerBusy" == ex.RequestInformation.ErrorCode || "InternalError" == ex.RequestInformation.ErrorCode || "OperationTimedOut" == ex.RequestInformation.ErrorCode)
                    {
                        System.Threading.Thread.Sleep(sleepDuration);
                        sleepDuration *= 2;
                        continue;
                    }
                    else
                    {
                        throw;
                    }
                }

                Debug.WriteLine($"Successfully Put block List for blob {destBlob.Name}");

                break;
            }
        }

        private static string CreateBlockforBlob(CloudBlockBlob destBlob, int blockIndex, byte[] prevBuff, int numBytes)
        {
            string md5ChecksumString;
            string newBlockId;

            using (var shaHasher = new SHA384Managed())
            {
                using (var hasher = new MD5CryptoServiceProvider())
                {
                    var hashBasis = Encoding.UTF8.GetBytes(string.Concat(destBlob.Name, blockIndex));
                    newBlockId = Convert.ToBase64String(shaHasher.ComputeHash(hashBasis));

                    md5ChecksumString = Convert.ToBase64String(hasher.ComputeHash(prevBuff, 0, numBytes));
                }
            }

            using (var destMemStream = new MemoryStream(prevBuff, 0, numBytes))
            {
                int sleepDuration = 1000;

                while (true)
                {
                    try
                    {
                        destBlob.PutBlockAsync(newBlockId, destMemStream, md5ChecksumString).GetAwaiter().GetResult();
                    }
                    catch (StorageException ex)
                    {
                        Debug.WriteLine($"Error received: {ex.Message}");
                        Debug.WriteLine($"Block length was {numBytes}");

                        foreach (var addDetails in ex.RequestInformation.ExtendedErrorInformation.AdditionalDetails)
                        {
                            Debug.WriteLine($"{addDetails.Key}: {addDetails.Value}");
                        }

                        if ("ServerBusy" == ex.RequestInformation.ErrorCode || "InternalError" == ex.RequestInformation.ErrorCode || "OperationTimedOut" == ex.RequestInformation.ErrorCode)
                        {
                            destMemStream.Position = 0;

                            Thread.Sleep(sleepDuration);
                            sleepDuration *= 2;
                            continue;
                        }
                        else
                        {
                            throw;
                        }
                    }

                    Debug.WriteLine($"Successfully Put block with ID {newBlockId}");

                    return newBlockId;
                }
            }
        }

        private static int FindLastInstanceOfDelimiter(byte[] buff, char delimiter)
        {
            for (int offset = buff.Length - 1; offset > 0; offset--)
            {
                if (buff[offset] == delimiter)
                {
                    return offset;
                }
            }

            // TODO handle cases where we rewind all the way back to 0 - that means the delimiter was not found
            return -1;
        }
    }
}
