//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using CosmosScale;
//using Microsoft.Azure.Documents.Client;
//using System;
//using System.Collections.Concurrent;
//using System.Threading.Tasks;
//using System.Linq;
//using System.Collections.Generic;
//using System.Diagnostics;

//namespace CosmosScale.Tests
//{
//    [TestClass]
//    public class DeleteTests
//    {
//        private DocumentClient _client;
//        private CosmosScaleOperator _cosmosOperator;

//        public DeleteTests()
//        {
//            TextWriterTraceListener tr2 = new TextWriterTraceListener(System.IO.File.CreateText($"{DateTime.Now.Ticks}_{Guid.NewGuid()}.txt"));
//            Trace.Listeners.Add(tr2);
//            Trace.AutoFlush = true;

//            _client = new DocumentClient(new Uri(Constants.COSMOS_URI), Constants.COSMOS_PASS,
//              new ConnectionPolicy
//              {
//                  ConnectionMode = ConnectionMode.Direct,
//                  ConnectionProtocol = Protocol.Tcp,
//                  RequestTimeout = TimeSpan.FromMinutes(2)
//              });

//            _cosmosOperator = new CosmosScaleOperator(400, 20000, "Test", "CollectionTest", _client);
//        }

//        [TestMethod]
//        public void DeleteTop100()
//        {
//            var items = _cosmosOperator.QueryCosmos<CosmosTestRetrieveOperationObject>("SELECT TOP 100 * FROM c");

//            var results = new List<CosmosOperationResponse>();
//            Parallel.ForEach(items, item =>
//            {
//                results.Add(_cosmosOperator.DeleteSingleDocumentAsync(item.id).GetAwaiter().GetResult());
//            });

//            CheckAsertResult(results);
            
//        }

//        [TestMethod]
//        public void DeleteTop100K()
//        {
//            var items = _cosmosOperator.QueryCosmos<CosmosTestRetrieveOperationObject>("SELECT TOP 100000 * FROM c");

//            var results = new List<CosmosOperationResponse>();
//            Parallel.ForEach(items, item =>
//            {
//                _cosmosOperator.DeleteSingleDocumentAsync(item.id).GetAwaiter().GetResult();
//            });

//            CheckAsertResult(results);
//        }

//        [TestMethod]
//        public void DeleteAll()
//        {
//            var items = _cosmosOperator.QueryCosmos<CosmosTestRetrieveOperationObject>("SELECT * FROM c");

//            var results = new List<CosmosOperationResponse>();
//            Parallel.ForEach(items, item =>
//            {
//                _cosmosOperator.DeleteSingleDocumentAsync(item.id).GetAwaiter().GetResult();
//            });

//            CheckAsertResult(results);
//        }
//        [TestMethod]
//        public async Task DeleteAllNoResult()
//        {
//            var items = _cosmosOperator.QueryCosmos<CosmosTestRetrieveOperationObject>("SELECT * FROM c");

//            List<Task> tasks = new List<Task>();
//            foreach (var item in items)
//            {
//                tasks.Add(_cosmosOperator.DeleteSingleDocumentAsync(item.id));
//            }

//            await Task.WhenAll(tasks);
//        }

//        private void CheckAsertResult(List<CosmosOperationResponse> results)
//        {
//            var unsuccessCount = results.Where(r => r.Success == false).Count(); //# of items that didnt insert

//            var allScales = new List<ScaleOperation>();
//            foreach (var item in results)
//            {
//                allScales.AddRange(item.ScaleOperations);
//            }

//            var scaleCount = allScales.Count(); //# of scale operations

//            Assert.AreEqual(unsuccessCount, 0);
//        }
//    }
//}
