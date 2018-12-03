using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using CosmosScale.Tests;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Documents.Client;

namespace CosmosScale.WebTest.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class InsertController : ControllerBase
    {
        private DocumentClient _client;
        private CosmosScaleOperator _cosmosOperator;

        public InsertController()
        {
            _client = new DocumentClient(new Uri("https://cosmosscaletest.documents.azure.com:443/"), "vsHDH7Oa1Vstpbg6k3t7dJsvQzkDvlXssTMM4MWdaUw3Iyofprh9bRvVLRn2ggr86WhV7icgRJaVBVkJTBWRmg==",
                new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp,
                    RequestTimeout = TimeSpan.FromMinutes(2)
                });

            _cosmosOperator = new CosmosScaleOperator(400, 20000, "Test", "CollectionTest", _client);
        }

        // GET api/values
        [HttpGet]
        public ActionResult Insert50K()
        {
            Random rd = new Random();
            for (int i = 0; i < 10000000; i++)
            {
                _cosmosOperator.InsertDocumentAsync(new CosmosTestOperationObject
                {
                    SomeRandomProperty = rd.Next(1, 300000),
                    SomeRandomProperty2 = rd.Next(1, 100)
                });
            }

            return Ok();
        }
    }
}
