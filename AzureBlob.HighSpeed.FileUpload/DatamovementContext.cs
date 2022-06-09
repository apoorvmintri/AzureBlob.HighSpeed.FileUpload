using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.DataMovement;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace AzureBlob.HighSpeed.FileUpload
{
    public class DatamovementContext : IDisposable
    {
        private bool _isDisposed;
        private readonly SingleTransferContext _transferContext;
        private readonly object lockObj = new();
        private CloudBlobContainer _containerClient = null!;
        private int _blockSize = 4096;

        public DatamovementContext()
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

            var filePath = $"datamovement/{file.FileName}";
            var blockBlob = _containerClient.GetBlockBlobReference(filePath);

            using var stream = new MemoryStream();
            lock (lockObj)
            {
                file.CopyTo(stream);
            }
            stream.Position = 0;

            await TransferManager.UploadAsync(stream, blockBlob, null, _transferContext);
            return blockBlob.Uri.ToString();
        }

        public async Task<IEnumerable<string>> BlockUploadBatch(string containerName, IFormFileCollection files)
        {
            var uris = new ConcurrentBag<string>();
            _containerClient = await GetContainerClient(containerName);

            await Parallel.ForEachAsync(files, async (file, token) =>
            {
                var uri = await BlockUpload(containerName, file);
                uris.Add(uri);
            });

            return uris;
        }

        public async Task<string> BlockUpload(string containerName, IFormFile file)
        {
            if (_containerClient == null)
                _containerClient = await GetContainerClient(containerName);

            var filePath = $"blockdatamovement/{file.FileName}";
            var client = _containerClient.GetBlockBlobReference(filePath);

            using var stream = new MemoryStream();
            lock (lockObj)
            {
                file.CopyTo(stream);
            }
            stream.Position = 0;

            byte[] buffer;
            var arrayList = new ArrayList();
            var bytesLeft = (stream.Length - stream.Position);

            while (bytesLeft > 0)
            {
                if (bytesLeft >= _blockSize)
                {
                    buffer = new byte[_blockSize];
                    await stream.ReadAsync(buffer, 0, _blockSize);
                }
                else
                {
                    buffer = new byte[bytesLeft];
                    await stream.ReadAsync(buffer, 0, Convert.ToInt32(bytesLeft));
                    bytesLeft = (stream.Length - stream.Position);
                }

                using (var ms = new MemoryStream(buffer))
                {
                    var block = Convert.ToBase64String(Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()));
                    arrayList.Add(block);
                    await client.PutBlockAsync(block, ms, null);
                }

                bytesLeft = (stream.Length - stream.Position);
            }

            var blockArray = (string[])arrayList.ToArray(typeof(string));
            await client.PutBlockListAsync(blockArray);
            return client.Uri.ToString();
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
