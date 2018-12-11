using Microsoft.Azure.CosmosDB.BulkExecutor;
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
    public class BenchmarkingBulkTests
    {
        private static DocumentClient _client;
        private static CosmosScaleOperator _cosmosOperator;
        private static Uri _collectionUri;
        private static Random rd = new Random();

        private const string dbName = "CollectionBenchmarkingTest";
        private const string InsertCollectionName = "RegularInsert";

        private const int totalLoopCount = 3;

        private const int minimumRU = 400;
        private const int maximumRU = 35000;

        private static ConcurrentBag<CosmosTestOperationObject> insertObjects = new ConcurrentBag<CosmosTestOperationObject>();
       

        public BenchmarkingBulkTests()
        {
            _client = new DocumentClient(new Uri(Constants.COSMOS_URI), Constants.COSMOS_PASS,
              new ConnectionPolicy
              {
                  ConnectionMode = ConnectionMode.Direct,
                  ConnectionProtocol = Protocol.Tcp,
                  RequestTimeout = TimeSpan.FromMinutes(5)
              });

            _cosmosOperator = new CosmosScaleOperator(minimumRU, maximumRU, dbName, InsertCollectionName, _client);
            _cosmosOperator.Initialize().Wait();

            _collectionUri = UriFactory.CreateDocumentCollectionUri(dbName, InsertCollectionName);
        }
        
      
        [TestMethod]
        public async Task InsertIncrementBulkCAS()
        {
            List<InsertResult> insertResults = new List<InsertResult>();

            TextWriterTraceListener tr = new TextWriterTraceListener(System.IO.File.CreateText("scale.txt"));
            Trace.Listeners.Add(tr);
            Trace.AutoFlush = true;

            var res5k = await InsertAsBulkCAS(5000);
            insertResults.Add(res5k);
            Thread.Sleep(TimeSpan.FromMinutes(3));

            var res10K = await InsertAsBulkCAS(10000);
            insertResults.Add(res10K);
            Thread.Sleep(TimeSpan.FromMinutes(3));

            var res50K = await InsertAsBulkCAS(50000);
            insertResults.Add(res50K);
            Thread.Sleep(TimeSpan.FromMinutes(3));

            var res100K = await InsertAsBulkCAS(100000);
            insertResults.Add(res100K);
            Thread.Sleep(TimeSpan.FromMinutes(3));

            var res500K = await InsertAsBulkCAS(500000);
            insertResults.Add(res500K);
            Thread.Sleep(TimeSpan.FromMinutes(5));

            Trace.Listeners.Remove(tr);

            ReportResults(insertResults, "InsertAsBulkCASReport.txt");
        }

        [TestMethod]
        public async Task InsertIncrementBulk()
        {
            List<InsertResult> insertResults = new List<InsertResult>();

            var res5k = await InsertAsBulk(5000);
            insertResults.Add(res5k);
            Thread.Sleep(TimeSpan.FromMinutes(3));

            var res10K = await InsertAsBulk(10000);
            insertResults.Add(res10K);
            Thread.Sleep(TimeSpan.FromMinutes(3));

            var res50K = await InsertAsBulk(50000);
            insertResults.Add(res50K);
            Thread.Sleep(TimeSpan.FromMinutes(3));

            var res100K = await InsertAsBulk(100000);
            insertResults.Add(res100K);
            Thread.Sleep(TimeSpan.FromMinutes(3));

            var res500K = await InsertAsBulk(500000);
            insertResults.Add(res500K);
            Thread.Sleep(TimeSpan.FromMinutes(3));

            ReportResults(insertResults, "InsertAsBulkReport.txt");
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
        private async Task<InsertResult> InsertAsBulkCAS(int count)
        {
            var totalInsertedStart = (_cosmosOperator.QueryCosmos<long>("select value count(1) from c")).FirstOrDefault();

            InsertResult result = new InsertResult();
            result.Count = count;

            DocumentCollection cl = await _client.ReadDocumentCollectionAsync(_collectionUri);

            insertObjects = new ConcurrentBag<CosmosTestOperationObject>();
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
                
        private async Task<InsertResult> InsertAsBulk(int count)
        {
            var totalInsertedStart = (_cosmosOperator.QueryCosmos<long>("select value count(1) from c")).FirstOrDefault();

            InsertResult result = new InsertResult();
            result.Count = count;

            DocumentCollection cl = await _client.ReadDocumentCollectionAsync(_collectionUri);

            BulkExecutor be = new BulkExecutor(_client, cl);
            await be.InitializeAsync();

            insertObjects = new ConcurrentBag<CosmosTestOperationObject>();
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

            await be.BulkImportAsync(insertObjects);

            st.Stop();

            var totalInserted = (_cosmosOperator.QueryCosmos<long>("select value count(1) from c")).FirstOrDefault();

            result.TotalRunTime = st.Elapsed.TotalSeconds;
            result.SuccessRate = ((totalInserted - totalInsertedStart) / (double)count) * 100;
            return result;
        }

        private void DeleteAllDocuments()
        {
            _cosmosOperator.BulkDeleteDocuments("select * from c");
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
