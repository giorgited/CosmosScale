//using Microsoft.Azure.Documents.Client;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using System;
//using System.Collections.Generic;
//using System.Diagnostics;
//using System.Text;
//using System.Threading;

//namespace CosmosScale.Tests
//{
//    [TestClass]
//    public class TimerThreadTest
//    {
//        private DocumentClient _client;
//        private CosmosScaleOperator _cosmosOperator;

//        public TimerThreadTest()
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

//            _cosmosOperator = new CosmosScaleOperator(400, 20000, "Test", "CollectionTest", _client);
//        }


//        [TestMethod]
//        public void TimerTest()
//        {
//            _cosmosOperator = new CosmosScaleOperator(400, 20000, "Test", "CollectionTest", _client);
//            _cosmosOperator.QueryCosmos<dynamic>("select top 1 * from c");


//            _cosmosOperator = new CosmosScaleOperator(400, 20000, "Test", "CollectionTest", _client);
//            _cosmosOperator.QueryCosmos<dynamic>("select top 1 * from c");


//            _cosmosOperator = new CosmosScaleOperator(400, 20000, "Test", "CollectionTest", _client);
//            _cosmosOperator.QueryCosmos<dynamic>("select top 1 * from c");


//            _cosmosOperator = new CosmosScaleOperator(400, 20000, "Test", "CollectionTest", _client);
//            _cosmosOperator.QueryCosmos<dynamic>("select top 1 * from c");


//            _cosmosOperator = new CosmosScaleOperator(400, 20000, "Test", "CollectionTest", _client);
//            _cosmosOperator.QueryCosmos<dynamic>("select top 1 * from c");

//            Thread.Sleep((int)TimeSpan.FromMinutes(5).TotalMilliseconds);
//        }

//    }
//}
