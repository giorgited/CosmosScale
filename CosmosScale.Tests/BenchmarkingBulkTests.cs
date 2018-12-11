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

        private const int totalLoopCount = 5;

        private static ConcurrentBag<CosmosTestOperationObject> insertObjects = new ConcurrentBag<CosmosTestOperationObject>();
       

        public BenchmarkingBulkTests()
        {
            Initialize();
            _cosmosOperator.Initialize().Wait();

            _collectionUri = UriFactory.CreateDocumentCollectionUri(dbName, InsertCollectionName);

            Parallel.For(0, 100000, (x) =>
            {
                insertObjects.Add(new CosmosTestOperationObject
                {
                    SomeRandomProperty = rd.Next(1, 300000),
                    SomeRandomProperty2 = rd.Next(1, 100)
                });
            });
        }

        private void Initialize()
        {
            _client = new DocumentClient(new Uri(Constants.COSMOS_URI), Constants.COSMOS_PASS,
                  new ConnectionPolicy
                  {
                      ConnectionMode = ConnectionMode.Direct,
                      ConnectionProtocol = Protocol.Tcp,
                      RequestTimeout = TimeSpan.FromMinutes(5)
                  });

            _cosmosOperator = new CosmosScaleOperator(400, 10000, dbName, InsertCollectionName, _client);
        }


        [TestMethod]
        public async Task Insert5KBulk()
        {
            await InsertAsBulk(insertObjects.Take(5000), false);
        }
        [TestMethod]
        public async Task Insert5KBulkCAS()
        {
            Parallel.For(0, 5, (i) =>
            {
                InsertAsBulk(insertObjects.Take(5000), true).Wait();
            });
        }

        [TestMethod]
        public async Task Insert50KBulk()
        {
            await InsertAsBulk(insertObjects.Take(50000), false);
        }
        [TestMethod]
        public async Task Insert50KBulkCAS()
        {
            await InsertAsBulk(insertObjects.Take(50000), true);
        }

        [TestMethod]
        public async Task Insert100KBulk()
        {
            await InsertAsBulk(insertObjects.Take(100000), false);
        }
        [TestMethod]
        public async Task Insert100KBulkCAS()
        {
            await InsertAsBulk(insertObjects.Take(100000), true);
        }

        [TestMethod]
        public async Task Insert500KBulk()
        {
            await InsertAsBulk(insertObjects.Take(500000), false);
        }
        [TestMethod]
        public async Task Insert500KBulkCAS()
        {
            await InsertAsBulk(insertObjects.Take(500000), true);
        }


        private async Task InsertAsBulk(IEnumerable<CosmosTestOperationObject> list, bool useCAS = true)
        {
            StringBuilder fileName = new StringBuilder();
            fileName.Append(useCAS ? "InsertAsBulkCAS" : "InsertAsBulk");
            fileName.Append($"{list.Count()}-{DateTime.Now.Ticks}.txt");

            TextWriterTraceListener tr = new TextWriterTraceListener(System.IO.File.CreateText(fileName.ToString()));
            Trace.Listeners.Add(tr);
            Trace.AutoFlush = true;

            List<double> totalSeconds = new List<double>();
            List<double> totalInsert = new List<double>();

            DocumentCollection cl = await _client.ReadDocumentCollectionAsync(_collectionUri);

            BulkExecutor be = new BulkExecutor(_client, cl);
            await be.InitializeAsync();

            for (int i = 0; i < totalLoopCount; i++)
            {
                await deleteRecreateCollection();

                Stopwatch st = new Stopwatch();
                st.Start();

                if (useCAS)
                {
                    _cosmosOperator.BulkInsertDocuments(list);
                }
                else
                {
                    await be.BulkImportAsync(list);
                }

                st.Stop();

                Initialize();
                var totalInserted = (_cosmosOperator.QueryCosmos<long>("select value count(1) from c")).FirstOrDefault();
                Trace.WriteLine($"Run {0 + 1}: Inserted items in: {st.Elapsed.TotalSeconds}seconds, success rate: {(totalInserted / (double)list.Count()) * 100}");

                totalSeconds.Add(st.Elapsed.TotalSeconds);
                totalInsert.Add((totalInserted / (double)list.Count()) * 100);
            }

            Trace.WriteLine($"AvgRunTime: {totalSeconds.Average()}, AvgSuccessRate: {totalInsert.Average()}");
            Trace.Listeners.Remove(tr);
        }


        private async Task deleteRecreateCollection()
        {
            await _client.DeleteDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(dbName, InsertCollectionName));

            DocumentCollection documentCollection = new DocumentCollection();
            //documentCollection.PartitionKey.Paths.Add("/id");
            documentCollection.Id = InsertCollectionName;

            await _client.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(dbName), documentCollection);

        }
    }
}
