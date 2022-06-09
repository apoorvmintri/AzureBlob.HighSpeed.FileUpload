using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureBlob.HighSpeed.FileUpload
{
    public class StorageContext : IDisposable
    {
        private bool _isDisposed;
        private readonly object lockObj = new();
        private BlobContainerClient _containerClient = null;
        private int _blockSize = 4096;

        private BlobContainerClient GetContainerClient()
        {
            var connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            var containerClient = new BlobContainerClient(connectionString, "upload-test");
            return containerClient;
        }

        public async Task<IEnumerable<string>> UploadBatch(IFormFileCollection files)
        {
            var uris = new ConcurrentBag<string>();
            _containerClient = GetContainerClient();

            await Parallel.ForEachAsync(files, async (file, token) =>
            {
                var uri = await Upload(file);
                uris.Add(uri);
            });

            return uris;
        }

        public async Task<string> Upload(IFormFile file)
        {
            if (_containerClient == null)
                _containerClient = GetContainerClient();

            var filePath = $"storage/{file.FileName}";
            var client = _containerClient.GetBlobClient(filePath);

            using var stream = new MemoryStream();
            lock (lockObj)
            {
                file.CopyTo(stream);
            }
            stream.Position = 0;

            var response = await client.UploadAsync(stream, overwrite: true);
            return client.Uri.ToString();
        }

        public async Task<IEnumerable<string>> BlockUploadBatch(IFormFileCollection files)
        {
            var uris = new ConcurrentBag<string>();
            _containerClient = GetContainerClient();

            await Parallel.ForEachAsync(files, async (file, token) =>
            {
                var uri = await BlockUpload(file);
                uris.Add(uri);
            });

            return uris;
        }

        public async Task<string> BlockUpload(IFormFile file)
        {
            if (_containerClient == null)
                _containerClient = GetContainerClient();

            var filePath = $"blockstorage/{file.FileName}";
            var client = _containerClient.GetBlockBlobClient(filePath);

            using var stream = new MemoryStream();
            lock (lockObj)
            {
                file.CopyTo(stream);
            }
            stream.Position = 0;

            byte[] buffer;
            var arrayList = new ArrayList();
            var bytesLeft = (stream.Length - stream.Position);

            while(bytesLeft > 0)
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
                    await client.StageBlockAsync(block, ms);
                }

                bytesLeft = (stream.Length - stream.Position);
            }

            var blockArray = (string[])arrayList.ToArray(typeof(string));
            await client.CommitBlockListAsync(blockArray);
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
