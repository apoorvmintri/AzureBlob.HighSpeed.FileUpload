using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Diagnostics;

namespace AzureBlob.HighSpeed.FileUpload
{
    public static class FileUploadFunction
    {
        [FunctionName("DatamovementUpload")]
        public static async Task<IActionResult> DatamovementUpload(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "datamovement/upload")] HttpRequest req,
            ILogger log)
        {
            var watch = Stopwatch.StartNew();
            using var context = new DatamovementContext();
            var response = new List<string>();

            if (req.Form.Files.Count > 1)
                response.AddRange(await context.UploadBatch("upload-test", req.Form.Files));
            else
                response.Add(await context.Upload("upload-test", req.Form.Files[0]));

            watch.Stop();
            var message = $"Datamovement: Uploading {GetTotalUploadSize(req.Form.Files)} bytes: {watch.ElapsedMilliseconds / 1000} seconds";
            log.LogDebug(message);

            return new OkObjectResult(response);
        }

        [FunctionName("StorageUpload")]
        public static async Task<IActionResult> StorageUpload(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "storage/upload")] HttpRequest req,
            ILogger log)
        {
            var watch = Stopwatch.StartNew();
            using var context = new StorageContext();
            var response = new List<string>();

            if (req.Form.Files.Count > 1)
                response.AddRange(await context.UploadBatch(req.Form.Files));
            else
                response.Add(await context.Upload(req.Form.Files[0]));

            watch.Stop();
            var message = $"Datamovement: Uploading {GetTotalUploadSize(req.Form.Files)} bytes: {watch.ElapsedMilliseconds / 1000} seconds";
            log.LogDebug(message);

            return new OkObjectResult(response);
        }

        [FunctionName("BlockStorageUpload")]
        public static async Task<IActionResult> BlockStorageUpload(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "blockstorage/upload")] HttpRequest req,
            ILogger log)
        {
            var watch = Stopwatch.StartNew();
            using var context = new StorageContext();
            var response = new List<string>();

            if (req.Form.Files.Count > 1)
                response.AddRange(await context.BlockUploadBatch(req.Form.Files));
            else
                response.Add(await context.BlockUpload(req.Form.Files[0]));

            watch.Stop();
            var message = $"Block Storage: Uploading {GetTotalUploadSize(req.Form.Files)} bytes: {watch.ElapsedMilliseconds / 1000} seconds";
            log.LogDebug(message);

            return new OkObjectResult(response);
        }

        [FunctionName("BlockDatamovementUpload")]
        public static async Task<IActionResult> BlockDatamovementUpload(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "blockdatamovement/upload")] HttpRequest req,
            ILogger log)
        {
            var watch = Stopwatch.StartNew();
            using var context = new DatamovementContext();
            var response = new List<string>();

            if (req.Form.Files.Count > 1)
                response.AddRange(await context.BlockUploadBatch("upload-test", req.Form.Files));
            else
                response.Add(await context.BlockUpload("upload-test", req.Form.Files[0]));

            watch.Stop();
            var message = $"Block Datamovement: Uploading {GetTotalUploadSize(req.Form.Files)} bytes: {watch.ElapsedMilliseconds / 1000} seconds";
            log.LogDebug(message);

            return new OkObjectResult(response);
        }

        private static long GetTotalUploadSize(IFormFileCollection files)
        {
            long size = 0;

            foreach(var file in files)
                size += file.Length;

            return size;
        }
    }
}
