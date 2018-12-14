//using Microsoft.Azure.Documents.Client;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using System;
//using System.Collections.Generic;
//using System.Diagnostics;
//using System.Text;
//using System.Threading.Tasks;

//namespace CosmosScale.Tests
//{
//    [TestClass]
//    public class MutiCollectionTests
//    {
//        private DocumentClient _client;
//        private CosmosScaleOperator _cosmosOperator;

//        public MutiCollectionTests()
//        {
//            TextWriterTraceListener tr2 = new TextWriterTraceListener(System.IO.File.CreateText($"{DateTime.Now.Ticks}_{Guid.NewGuid()}.txt"));
//            Trace.Listeners.Add(tr2);
//            Trace.AutoFlush = true;

//            _client = new DocumentClient(new Uri(Constants.COSMOS_URI), Constants.COSMOS_PASS, new ConnectionPolicy
//              {
//                  ConnectionMode = ConnectionMode.Direct,
//                  ConnectionProtocol = Protocol.Tcp,
//                  RequestTimeout = TimeSpan.FromMinutes(2)
//              });
//        }

//        [TestMethod]
//        public async Task CreateMultipleResources()
//        {
//            var cosOperator = new CosmosScaleOperator(400, 20000, "Test2", "CollectionTest", _client);
//            await cosOperator.Initialize();


//            var cosOperator2 = new CosmosScaleOperator(400, 20000, "Test3", "CollectionTest", _client);
//            await cosOperator2.Initialize();


//            var cosOperator3 = new CosmosScaleOperator(400, 20000, "Test4", "CollectionTest", _client);
//            await cosOperator3.Initialize();
//        }
//    }
//}
