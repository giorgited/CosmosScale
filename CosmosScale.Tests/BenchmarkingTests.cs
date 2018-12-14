//using Microsoft.Azure.CosmosDB.BulkExecutor;
//using Microsoft.Azure.Documents;
//using Microsoft.Azure.Documents.Client;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using System;
//using System.Collections.Concurrent;
//using System.Collections.Generic;
//using System.Diagnostics;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//namespace CosmosScale.Tests
//{
//    [TestClass]
//    public class BenchmarkingTests
//    {
//        private static DocumentClient _client;
//        private static CosmosScaleOperator _cosmosOperator;
//        private static Uri _collectionUri;
//        private static Random rd = new Random();

//        private const string dbName = "CollectionBenchmarkingTest";
//        private const string InsertCollectionName = "RegularInsert";
        
//        private const int totalLoopCount = 3;

//        private static ConcurrentBag<CosmosTestOperationObject> insertObjects = new ConcurrentBag<CosmosTestOperationObject>();


//        public BenchmarkingTests()
//        {
//            Initialize();
//            _cosmosOperator.Initialize().Wait();

//            _collectionUri = UriFactory.CreateDocumentCollectionUri(dbName, InsertCollectionName);

//            Parallel.For(0, 100000, (i) =>
//            {
//                insertObjects.Add(new CosmosTestOperationObject
//                {
//                    SomeRandomProperty = rd.Next(1, 300000),
//                    SomeRandomProperty2 = rd.Next(1, 100)
//                });
//            });
//        }

//        private void Initialize()
//        {
//            _client = new DocumentClient(new Uri(Constants.COSMOS_URI), Constants.COSMOS_PASS,
//                  new ConnectionPolicy
//                  {
//                      ConnectionMode = ConnectionMode.Direct,
//                      ConnectionProtocol = Protocol.Tcp,
//                      RequestTimeout = TimeSpan.FromMinutes(5)
//                  });

//            _cosmosOperator = new CosmosScaleOperator(400, 25000, dbName, InsertCollectionName, _client);
//        }

//        #region 5K
//        [TestMethod]
//        public async Task Insert5kSingleThread()
//        {
//            await Insert(false, false, 5000);
//        }
//        [TestMethod]
//        public async Task Insert5kInParallel()
//        {
//            await Insert(true, false, 5000);
//        }
//        [TestMethod]
//        public async Task Insert5kSingleThreadViaCAS()
//        {
//            await Insert(false, true, 5000);
//        }
//        [TestMethod]
//        public async Task Insert5kInParallelViaCAS()
//        {
//            await Insert(true, true, 5000);
//        }
//        #endregion

//        #region 10K
//        [TestMethod]
//        public async Task Insert10kSingleThread()
//        {
//            await Insert(false, false, 10000);
//        }
//        [TestMethod]
//        public async Task Insert10kInParallel()
//        {
//            await Insert(true, false, 10000);
//        }
//        [TestMethod]
//        public async Task Insert10kSingleThreadViaCAS()
//        {
//            await Insert(false, true, 10000);
//        }
//        [TestMethod]
//        public async Task Insert10kInParallelViaCAS()
//        {
//            await Insert(true, true, 10000);
//        }
//        #endregion

//        #region 50K
//        [TestMethod]
//        public async Task Insert50kSingleThread()
//        {
//            await Insert(false, false, 50000);
//        }
//        [TestMethod]
//        public async Task Insert50kInParallel()
//        {
//            await Insert(true, false, 50000);
//        }
//        [TestMethod]
//        public async Task Insert50kSingleThreadViaCAS()
//        {
//            await Insert(false, true, 50000);
//        }
//        [TestMethod]
//        public async Task Insert50kInParallelViaCAS()
//        {
//            await Insert(true, true, 50000);
//        }
//        #endregion

//        #region 100K
//        [TestMethod]
//        public async Task Insert100kSingleThread()
//        {
//            await Insert(false, false, 100000);
//        }
//        [TestMethod]
//        public async Task Insert100kInParallel()
//        {
//            await Insert(true, false, 100000);
//        }
//        [TestMethod]
//        public async Task Insert100kSingleThreadViaCAS()
//        {
//            await Insert(false, true, 100000);
//        }
//        [TestMethod]
//        public async Task Insert100kInParallelViaCAS()
//        {
//            await Insert(true, true, 100000);
//        }
//        #endregion



//        private async Task Insert(bool inParallel, bool insertAsCAS, int count)
//        {
//            var list = insertObjects.Take(count);

//            StringBuilder fileName = new StringBuilder();
//            fileName.Append(inParallel ? "InsertInParallel" : "Insert");
//            fileName.Append(insertAsCAS ? "ViaCAS" : "");
//            fileName.Append($"{count}.txt");

//            TextWriterTraceListener tr = new TextWriterTraceListener(System.IO.File.CreateText(fileName.ToString()));
//            Trace.Listeners.Add(tr);
//            Trace.AutoFlush = true;

//            Trace.WriteLine("\n\n");
//            Trace.WriteLine($"Starting benchmarking of inserting {count} lines.");

//            if (inParallel)
//            {
//                await InsertInParallel(list, insertAsCAS);
//            }
//            else
//            {
//                await InsertInSingleThreadAsync(list, insertAsCAS);
//            }

//            Trace.Listeners.Remove(tr);
//        }

//        private async Task InsertInSingleThreadAsync(IEnumerable<CosmosTestOperationObject> list, bool insertAsCAS)
//        {
//            List<double> totalSeconds = new List<double>();
//            List<double> totalInsert = new List<double>();

//            for (int i = 0; i < totalLoopCount; i++)
//            {
//                await deleteRecreateCollection();

//                Stopwatch st = new Stopwatch();
//                st.Start();

//                foreach (var item in list)
//                {
//                    try
//                    {
//                        if (insertAsCAS)
//                        {
//                            await _cosmosOperator.InsertSingleDocumentAsync(item);
//                        }
//                        else
//                        {
//                            await _client.CreateDocumentAsync(_collectionUri, item);
//                        }
//                    }
//                    catch { }
//                }               

//                st.Stop();

//                Initialize();
//                var totalInserted = (_cosmosOperator.QueryCosmos<long>("select value count(1) from c")).FirstOrDefault();
//                Trace.WriteLine($"Run {i + 1}: Inserted items in: {st.Elapsed.TotalSeconds}seconds, success rate: {(totalInserted / (double)list.Count()) * 100}");

//                totalSeconds.Add(st.Elapsed.TotalSeconds);
//                totalInsert.Add((totalInserted / (double)list.Count()) * 100);
//            }

//            Trace.WriteLine($"AvgRunTime: {totalSeconds.Average()}, AvgSuccessRate: {totalInsert.Average()}");
//        }

//        private async Task InsertInParallel(IEnumerable<CosmosTestOperationObject> list, bool insertAsCAS)
//        {
//            List<double> totalSeconds = new List<double>();
//            List<double> totalInsert = new List<double>();

//            for (int i = 0; i < totalLoopCount; i++)
//            {
//                await deleteRecreateCollection();

//                Stopwatch st = new Stopwatch();
//                st.Start();

//                List<Task> tasks = new List<Task>();

//                if (insertAsCAS)
//                {
//                    tasks.AddRange(list.AsParallel().Select(l => _cosmosOperator.InsertSingleDocumentAsync(l).ContinueWith(c => c)));                    
//                }
//                else
//                {
//                    foreach (var item in list)
//                    {
//                        tasks.Add(_client.CreateDocumentAsync(_collectionUri, item).ContinueWith(c => c));
//                    }
//                }
                
//                await Task.WhenAll(tasks);

//                st.Stop();

//                Initialize();
//                var totalInserted = (_cosmosOperator.QueryCosmos<long>("select value count(1) from c")).FirstOrDefault();
//                Trace.WriteLine($"Run {i + 1}: Inserted items in: {st.Elapsed.TotalSeconds}seconds, success rate: {(totalInserted / (double)list.Count()) * 100}");

//                totalSeconds.Add(st.Elapsed.TotalSeconds);
//                totalInsert.Add((totalInserted / (double)list.Count()) * 100);
//            }

//            Trace.WriteLine($"AvgRunTime: {totalSeconds.Average()}, AvgSuccessRate: {totalInsert.Average()}");
//        }

//        private async Task deleteRecreateCollection()
//        {
//            await _client.DeleteDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(dbName, InsertCollectionName));

//            DocumentCollection documentCollection = new DocumentCollection();
//            documentCollection.Id = InsertCollectionName;

//            await _client.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(dbName), documentCollection);

//        }
//        private async Task ScaleDownCollectionAsync(int minRu)
//        {
//            Database database = _client.CreateDatabaseQuery($"SELECT * FROM d WHERE d.id = \"{dbName}\"").AsEnumerable().First();

//            List<DocumentCollection> collections = _client.CreateDocumentCollectionQuery((String)database.SelfLink).ToList();

//            foreach (var collection in collections)
//            {
//                if (collection.Id == InsertCollectionName)
//                {
//                    Offer offer = _client.CreateOfferQuery()
//                        .Where(r => r.ResourceLink == collection.SelfLink)
//                        .AsEnumerable()
//                        .SingleOrDefault();

//                    offer = new OfferV2(offer, minRu);

//                    await _client.ReplaceOfferAsync(offer);
//                }
//            }
//        }
//    }


//    public class docCountResult
//    {
//        public long count { get; set; }
//    }
//}
