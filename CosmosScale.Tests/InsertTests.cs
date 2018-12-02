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
    public class InsertTests
    {
        private DocumentClient _client;
        private CosmosScaleOperator _cosmosOperator;

        public InsertTests()
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
        public void InsertOne()
        {
            var results = InsertDocuments(1);
            CheckAsertResult(results);
        }

        [TestMethod]
        public void Insert10K()
        {
            var results = InsertDocuments(10000);
            CheckAsertResult(results);
        }

        [TestMethod]
        public void Insert20K()
        {
            var results = InsertDocuments(20000);
            CheckAsertResult(results);
        }

        [TestMethod]
        public void Insert100K()
        {
            var results = InsertDocuments(100000);
            CheckAsertResult(results);
        }

        [TestMethod]
        public void Insert500K()
        {
            var results = InsertDocuments(500000);
            CheckAsertResult(results);
        }

        [TestMethod]
        public void Insert1M()
        {
            var results = InsertDocuments(1000000);            
            CheckAsertResult(results);
        }

        [TestMethod]
        public void Insert50M()
        {
            var results = InsertDocuments(50000000);
            CheckAsertResult(results);
        }

        private List<CosmosOperationResponse> InsertDocuments(int count)
        {     
            ConcurrentBag<CosmosTestOperationObject> list = new ConcurrentBag<CosmosTestOperationObject>();
            Random rd = new Random();
            Parallel.For(0, count, (i) =>
            {
                list.Add(new CosmosTestOperationObject
                {
                    SomeRandomProperty = rd.Next(1, 300000),
                    SomeRandomProperty2 = rd.Next(1, 100)
                });
            });

            ConcurrentBag<CosmosOperationResponse> results = new ConcurrentBag<CosmosOperationResponse>();

            Parallel.ForEach(list, item =>
            {
                results.Add(_cosmosOperator.InsertDocumentAsync(item).GetAwaiter().GetResult());
            });

            return results.ToList();
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
