using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureBlob.HighSpeed.FileUpload
{
    public static class DetailsFunction
    {
        public static IActionResult GetThreads(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "get/threads")] HttpRequest req,
            ILogger log)
        {
            var response = $"Processor Count: {Environment.ProcessorCount}\nThreads: {Environment.ProcessorCount * 8}";
            return new OkObjectResult(response);
        }
    }
}
