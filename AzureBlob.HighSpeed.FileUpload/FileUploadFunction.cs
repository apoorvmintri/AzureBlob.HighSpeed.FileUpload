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

namespace AzureBlob.HighSpeed.FileUpload
{
    public static class FileUploadFunction
    {
        [FunctionName("UploadFile")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "fast/upload")] HttpRequest req,
            ILogger log)
        {
            var context = new BlobContext();
            var response = new List<string>();

            if (req.Form.Files.Count > 1)
                response.AddRange(await context.UploadBatch("upload-test", req.Form.Files));
            else
                response.Add(await context.Upload("upload-test", req.Form.Files[0]));

            return new OkObjectResult(response);
        }
    }
}
