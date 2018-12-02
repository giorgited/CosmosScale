using Microsoft.Azure.Documents.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace CosmosScale.Tests
{
    [TestClass]
    public class TimerThreadTest
    {
        private DocumentClient _client;
        private CosmosScaleOperator _cosmosOperator;

        public TimerThreadTest()
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
        public void TimerTest()
        {
            _cosmosOperator.QueryCosmos<dynamic>("select top 1 * from c");

            Thread.Sleep((int)TimeSpan.FromMinutes(5).TotalMilliseconds);
        }

    }
}
