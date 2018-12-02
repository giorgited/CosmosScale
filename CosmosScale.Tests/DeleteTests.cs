using Microsoft.VisualStudio.TestTools.UnitTesting;
using CosmosScale;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;

namespace CosmosScale.Tests
{
    [TestClass]
    public class DeleteTests
    {
        private DocumentClient _client;
        private CosmosScaleOperator _cosmosOperator;

        public DeleteTests()
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

        [TestMethod]
        public void DeleteTop100()
        {
            var items = _cosmosOperator.QueryCosmos<CosmosTestRetrieveOperationObject>("SELECT TOP 100 * FROM c");

            var results = new List<CosmosOperationResponse>();
            Parallel.ForEach(items, item =>
            {
                results.Add(_cosmosOperator.DeleteDocumentAsync(item.id).GetAwaiter().GetResult());
            });

            CheckAsertResult(results);
            
        }

        [TestMethod]
        public void DeleteTop100K()
        {
            var items = _cosmosOperator.QueryCosmos<CosmosTestRetrieveOperationObject>("SELECT TOP 100000 * FROM c");

            var results = new List<CosmosOperationResponse>();
            Parallel.ForEach(items, item =>
            {
                _cosmosOperator.DeleteDocumentAsync(item.id).GetAwaiter().GetResult();
            });

            CheckAsertResult(results);
        }

        [TestMethod]
        public void DeleteAll()
        {
            var items = _cosmosOperator.QueryCosmos<CosmosTestRetrieveOperationObject>("SELECT * FROM c");

            var results = new List<CosmosOperationResponse>();
            Parallel.ForEach(items, item =>
            {
                _cosmosOperator.DeleteDocumentAsync(item.id).GetAwaiter().GetResult();
            });

            CheckAsertResult(results);
        }

        private void CheckAsertResult(List<CosmosOperationResponse> results)
        {
            var unsuccessCount = results.Where(r => r.Success == false).Count(); //# of items that didnt insert

            var allScales = new List<ScaleOperation>();
            foreach (var item in results)
            {
                allScales.AddRange(item.ScaleOperations);
            }

            var scaleCount = allScales.Count(); //# of scale operations

            Assert.AreEqual(unsuccessCount, 0);
        }




    }
    

    
}
