using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.DataMovement;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace AzureBlob.HighSpeed.FileUpload
{
    public class BlobContext : IDisposable
    {
        private bool _isDisposed;
        private readonly SingleTransferContext _transferContext;
        private readonly object lockObj = new();
        private CloudBlobContainer _containerClient = null!;

        public BlobContext()
        {
            var availableThreads = Environment.ProcessorCount * 8;
            TransferManager.Configurations.ParallelOperations = availableThreads;
            ServicePointManager.DefaultConnectionLimit = availableThreads;
            ServicePointManager.Expect100Continue = false;

            _transferContext = new SingleTransferContext();
            _transferContext.ShouldOverwriteCallbackAsync = TransferContext.ForceOverwrite;
        }

        private CloudBlobClient CreateBlobClient()
        {
            var connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            var account = CloudStorageAccount.Parse(connectionString);
            return account.CreateCloudBlobClient();
        }

        private async Task<CloudBlobContainer> GetContainerClient(string containerName)
        {
            var blobClient = CreateBlobClient();
            var client = blobClient.GetContainerReference(containerName);
            await client.CreateIfNotExistsAsync();
            return client;
        }

        public async Task<IEnumerable<string>> UploadBatch(string containerName, IFormFileCollection files)
        {
            var uris = new ConcurrentBag<string>();
            _containerClient = await GetContainerClient(containerName);

            await Parallel.ForEachAsync(files, async (file, token) =>
            {
                var uri = await Upload(containerName, file);
                uris.Add(uri);
            });

            return uris;
        }

        public async Task<string> Upload(string containerName, IFormFile file)
        {
            if (_containerClient == null)
                _containerClient = await GetContainerClient(containerName);

            var filePath = $"high-speed/{file.FileName}";
            var blockBlob = _containerClient.GetBlockBlobReference(filePath);

            using var stream = new MemoryStream();
            lock (lockObj)
            {
                file.CopyTo(stream);
            }
            await TransferManager.UploadAsync(stream, blockBlob, null, _transferContext);
            return blockBlob.Uri.ToString();
        }

        public async Task<bool> Delete(string containerName, string fileName)
        {
            if (_containerClient == null)
                _containerClient = await GetContainerClient(containerName);

            var filePath = $"high-speed/{fileName}";
            return await _containerClient.GetBlockBlobReference(filePath).DeleteIfExistsAsync();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_isDisposed)
                return;

            _isDisposed = true;
        }
    }
}
