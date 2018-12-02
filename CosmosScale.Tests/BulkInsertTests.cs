//using Microsoft.Azure.Documents.Client;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using System;
//using System.Collections.Concurrent;
//using System.Collections.Generic;
//using System.Text;
//using System.Threading.Tasks;

//namespace CosmosScale.Tests
//{
//    [TestClass]
//    public class BulkInsertTests
//    {
//        private DocumentClient _client;
//        private CosmosScaleOperator _cosmosOperator;
//        public BulkInsertTests()
//        {
//            _client = new DocumentClient(new Uri("https://cosmosscaletest.documents.azure.com:443/"), "vsHDH7Oa1Vstpbg6k3t7dJsvQzkDvlXssTMM4MWdaUw3Iyofprh9bRvVLRn2ggr86WhV7icgRJaVBVkJTBWRmg==",
//              new ConnectionPolicy
//              {
//                  ConnectionMode = ConnectionMode.Direct,
//                  ConnectionProtocol = Protocol.Tcp,
//                  RequestTimeout = TimeSpan.FromMinutes(2)
//              });

//            _cosmosOperator = new CosmosScaleOperator(400, 20000, "Test", "CollectionTest", _client);
//        }

//        [TestMethod]
//        public async Task BulkInsert10K()
//        {
//            ConcurrentBag<CosmosTestOperationObject> list = new ConcurrentBag<CosmosTestOperationObject>();
//            Random rd = new Random();
//            Parallel.For(0, 10000, (i) =>
//            {
//                list.Add(new CosmosTestOperationObject
//                {
//                    SomeRandomProperty = rd.Next(1, 300000),
//                    SomeRandomProperty2 = rd.Next(1, 100)
//                });
//            });

//            await _cosmosOperator.BulkInsertAsync(list);
//        }
//    }
//}
