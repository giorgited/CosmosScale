using CosmosScale.Models;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CosmosScale.Tests
{
    [TestClass]
    public class MetaDataOperatorTests
    {
        private static DocumentClient _client;
        private static Random rd = new Random();

        public MetaDataOperatorTests()
        {
            _client = new DocumentClient(new Uri(Constants.COSMOS_URI), Constants.COSMOS_PASS,
              new ConnectionPolicy
              {
                  ConnectionMode = ConnectionMode.Direct,
                  ConnectionProtocol = Protocol.Tcp,
                  RequestTimeout = TimeSpan.FromMinutes(5)
              });
        }

        [TestMethod]
        public async Task RunAsPermanentCollection()
        {
            List<InsertResult> insertResults = new List<InsertResult>();

            CosmosScaleOperator op = new CosmosScaleOperator(_client, 400, 5000, "test", "test1");
            await op.InitializeAsync(Models.StateMetaDataStorage.PermamentCosmosCollection);

            TextWriterTraceListener tr = new TextWriterTraceListener(System.IO.File.CreateText("scale.txt"));
            Trace.Listeners.Add(tr);
            Trace.AutoFlush = true;

            var res5k = await InsertAsBulkCAS(5000, UriFactory.CreateDocumentCollectionUri("test1", "test1"), op);
            insertResults.Add(res5k);
            ReportResults(insertResults, "runAsPermamentcollection");
            Thread.Sleep(TimeSpan.FromMinutes(3));
        }


        private void ReportResults(List<InsertResult> insertResults, string fileName)
        {
            //Report
            TextWriterTraceListener tr = new TextWriterTraceListener(System.IO.File.CreateText(fileName));
            Trace.Listeners.Add(tr);
            Trace.AutoFlush = true;

            var totalScales = new List<ScaleOperation>();
            foreach (var result in insertResults)
            {
                Trace.WriteLine($"Count: {result.Count}");
                Trace.WriteLine($"TotalRuntime: {result.TotalRunTime}");
                Trace.WriteLine($"SuccessRate: {result.SuccessRate}");
                totalScales.AddRange(result.BulkInsertOpeartionResult.ScaleOperations);
            }

            totalScales = totalScales.OrderBy(a => a.OperationTime).ToList();
            for (int i = 0; i < totalScales.Count; i++)
            {
                var scale = totalScales.ElementAt(i);
                if (scale.ScaledSuccess)
                {
                    Trace.WriteLine($"{i}: ScaledFrom: {scale.ScaledFrom}, ScaledTo: {scale.ScaledTo}, Time: {scale.OperationTime}");
                }
                else
                {
                    Trace.WriteLine($"{i}: FailReason: {scale.ScaleFailReason}, Time: {scale.OperationTime}");
                }
            }

            Trace.Listeners.Remove(tr);
        }

        private async Task<InsertResult> InsertAsBulkCAS(int count, Uri _collectionUri, CosmosScaleOperator _cosmosOperator)
        {
            var totalInsertedStart = (_cosmosOperator.QueryCosmos<long>("select value count(1) from c")).FirstOrDefault();

            InsertResult result = new InsertResult();
            result.Count = count;

            var insertObjects = new ConcurrentBag<CosmosTestOperationObject>();
            Parallel.For(0, count, (x) =>
            {
                insertObjects.Add(new CosmosTestOperationObject
                {
                    SomeRandomProperty = rd.Next(1, 300000),
                    SomeRandomProperty2 = rd.Next(1, 100)
                });
            });

            Stopwatch st = new Stopwatch();
            st.Start();

            result.BulkInsertOpeartionResult = _cosmosOperator.BulkInsertDocuments(insertObjects);

            st.Stop();

            var totalInserted = (_cosmosOperator.QueryCosmos<long>("select value count(1) from c")).FirstOrDefault();

            result.TotalRunTime = st.Elapsed.TotalSeconds;
            result.SuccessRate = ((totalInserted - totalInsertedStart) / (double)count) * 100;
            return result;
        }
    }

    public class InsertResult
    {
        public int Count { get; set; }
        public double TotalRunTime { get; set; }
        public double SuccessRate { get; set; }
        public BulkInsertOpeartionResult BulkInsertOpeartionResult { get; set; } = new BulkInsertOpeartionResult();
    }
}
