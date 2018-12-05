using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Timers;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

namespace CosmosScale
{
    public class CosmosScaleOperator
    {
        private static DocumentClient _client;
        private int _minRu, _maxRu;
        private Uri _collectionUri;
        private string _databaseName, _collectionName;
        private string _collectionPartitionKey;
        private const int _maximumRetryCount = 10;


        private static System.Timers.Timer aTimer;

        //Tuple<string,string> --> databaseName, collectionName
        //Tuple<DateTime, int> --> latestActivityDate, minRu
        private static ConcurrentDictionary<Tuple<string,string>, DateTime> latestActivty = new ConcurrentDictionary<Tuple<string, string>, DateTime>();

        //Tuple<string, string, int --> databaseName, collectionName, minRu
        private static ConcurrentBag<Tuple<string, string, int>> collectionsCacheAside = new ConcurrentBag<Tuple<string, string, int>>();
       
        public CosmosScaleOperator(int minimumRu, int maximumRu, string databaseName, string collectionName, DocumentClient client, string partitionKeyPropertyName = "/id")
        {
            _client = client;
            _minRu = minimumRu;
            _maxRu = maximumRu;
            _databaseName = databaseName;
            _collectionName = collectionName;
            _collectionPartitionKey = partitionKeyPropertyName;
            _collectionUri = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName);
            
            latestActivty[new Tuple<string, string>(_databaseName, _collectionName)] = DateTime.Now;

            if (collectionsCacheAside.Any(c => c.Item1 == databaseName && c.Item2 == collectionName))
            {
                Trace.WriteLine($"{databaseName}|{collectionName} already exists in collection. Skipping caching.");
                //item for this db and collection exist, min RU will NOT be replaced, min RU should be chosen globaly
            } else
            {
                collectionsCacheAside.Add(new Tuple<string, string, int>(databaseName, collectionName, minimumRu));
            }
        }
        static CosmosScaleOperator()
        {
            SetTimer();
        }

        public async Task InitializeResourcesAsync(RequestOptions databaseRequestOptions = null, RequestOptions collectionRequestOptions = null, string collectionPartitionProperty = null)
        {
            Database database = new Database();
            database.Id = _databaseName;
            var databaseUri = UriFactory.CreateDatabaseUri(_databaseName);

            DocumentCollection documentCollection = new DocumentCollection();
            documentCollection.Id = _collectionName;

            collectionPartitionProperty = collectionPartitionProperty ?? "/id";
            documentCollection.PartitionKey = new PartitionKeyDefinition() { Paths = new System.Collections.ObjectModel.Collection<string> { collectionPartitionProperty } };

            if (databaseRequestOptions == null)
            {
                await _client.CreateDatabaseIfNotExistsAsync(database);
            }
            else
            {
                await _client.CreateDatabaseIfNotExistsAsync(database, databaseRequestOptions);
            }
            
            if (collectionRequestOptions == null)
            {
               await  _client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, documentCollection, collectionRequestOptions);
            }
            else
            {
               await  _client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, documentCollection);
            }

            Trace.WriteLine($"Initialized Resources {_databaseName}, {_collectionName}");
        }
        

        public IEnumerable<T> QueryCosmos<T>(string cosmosSqlQuery, int maxItemCount = -1, bool QueryCrossPartition = true)
        {
            Trace.WriteLine($"Querying cosmos: {cosmosSqlQuery}, {maxItemCount}, {QueryCrossPartition}");

            RefreshLatestActvity();
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = maxItemCount, EnableCrossPartitionQuery = QueryCrossPartition };

            return _client.CreateDocumentQuery<T>(
                    _collectionUri,
                    new SqlQuerySpec()
                    {
                        QueryText = cosmosSqlQuery
                    },
                    queryOptions);
        }

        #region INSERT
        public async Task<CosmosOperationResponse> InsertDocumentAsync(object document)
        {
            //Trace.WriteLine($"Inserting: {JsonConvert.SerializeObject(document)}, in {_databaseName}|{_collectionName}");
            RefreshLatestActvity();
            CosmosOperationResponse result = new CosmosOperationResponse();
            return await InsertDocumentAsync(document, 0, result);
        }
        private async Task<CosmosOperationResponse> InsertDocumentAsync(object document, int retryCount, CosmosOperationResponse result)
        {
            try
            {
                await _client.CreateDocumentAsync(_collectionUri, document);
                result.Success = true;
                result.TotalRetries = retryCount;
                return result;
            }
            catch (Exception e)
            {
                if (e.Message.Contains("Request rate is large"))
                {
                    if (retryCount > _maximumRetryCount)
                    {
                        result.Success = false;
                        result.TotalRetries = retryCount;
                        return result;
                    }
                    else
                    {
                        var op = await ScaleLogic.ScaleUpCollectionAsync(_client, _databaseName, _collectionName, _minRu, _maxRu);
                        result.ScaleOperations.Add(op);
                        return await InsertDocumentAsync(document, retryCount++, result);
                    }
                }
                else
                {
                    throw;
                }
            }
        }
        #endregion

        #region DELETE
        public async Task<CosmosOperationResponse> DeleteDocumentAsync(string id, object partitionKey = null)
        {
            Trace.WriteLine($"Deleting: {id}, in {_databaseName}|{_collectionName}");

            RefreshLatestActvity();
            CosmosOperationResponse result = new CosmosOperationResponse();
            return await DeleteDocumentAsync(id, 0, result, partitionKey);
        }

        private async Task<CosmosOperationResponse> DeleteDocumentAsync(string id, int retryCount, CosmosOperationResponse result, object partitionKey = null)
        {
            try
            {
                var docUri = UriFactory.CreateDocumentUri(_databaseName, _collectionName, id);
                if (partitionKey == null)
                {
                    await _client.DeleteDocumentAsync(docUri, new RequestOptions() { PartitionKey = new PartitionKey(id) });
                }
                else
                {
                    await _client.DeleteDocumentAsync(docUri, new RequestOptions() { PartitionKey = new PartitionKey(partitionKey) });
                }
                result.Success = true;
                result.TotalRetries = retryCount;
                return result;
            }
            catch (Exception e)
            {
                if (e.Message.Contains("Request rate is large"))
                {
                    if (retryCount > _maximumRetryCount)
                    {
                        result.Success = false;
                        result.TotalRetries = retryCount;
                        return result;
                    }
                    else
                    {
                        var op = await ScaleLogic.ScaleUpCollectionAsync(_client, _databaseName, _collectionName, _minRu, _maxRu);
                        result.ScaleOperations.Add(op);
                        return await DeleteDocumentAsync(id, retryCount++, result, partitionKey);
                    }
                }
                else
                {
                    throw;
                }
            }
        }
        #endregion

        #region REPLACE
        public async Task<CosmosOperationResponse> ReplaceDocumentAsync(string oldDocumentId, object newDocument)
        {
            Trace.WriteLine($"Replacing {oldDocumentId} with {JsonConvert.SerializeObject(newDocument)}, in {_databaseName}|{_collectionName}");

            RefreshLatestActvity();
            CosmosOperationResponse result = new CosmosOperationResponse();
            return await ReplaceDocumentAsync(oldDocumentId, newDocument, 0, result);
        }
        private async Task<CosmosOperationResponse> ReplaceDocumentAsync(string oldDocumentId, object newDocument, int retryCount, CosmosOperationResponse result)
        {
            try
            {
                var docUri = UriFactory.CreateDocumentUri(_databaseName, _collectionName, oldDocumentId);
                await _client.ReplaceDocumentAsync(docUri, newDocument);
                result.Success = true;
                result.TotalRetries = retryCount;
                return result;
            }
            catch (Exception e)
            {
                if (e.Message.Contains("Request rate is large"))
                {
                    if (retryCount > _maximumRetryCount)
                    {
                        result.Success = false;
                        result.TotalRetries = retryCount;
                        return result;
                    }
                    else
                    {
                        var op = await ScaleLogic.ScaleUpCollectionAsync(_client, _databaseName, _collectionName, _minRu, _maxRu);
                        result.ScaleOperations.Add(op);
                        return await ReplaceDocumentAsync(oldDocumentId, newDocument, retryCount++, result);
                    }
                }
                else
                {
                    throw;
                }
            }
        }
        #endregion


        #region VOLLEYBALL SCALEDOWN
        private void RefreshLatestActvity()
        {
            if (_databaseName == null || _collectionName == null)
            {
                return;
            }

            latestActivty[new Tuple<string, string>(_databaseName, _collectionName)] = DateTime.Now;
        }
        private static void SetTimer()
        {
            // Create a timer with a 15 minute interval.
            aTimer = new Timer(TimeSpan.FromSeconds(10).TotalMilliseconds);
            aTimer.Elapsed += OnTimedEvent;
            aTimer.AutoReset = true;
            aTimer.Enabled = true;

            Trace.WriteLine($"Volleyball timer set.");
        }
        private static void OnTimedEvent(Object source, ElapsedEventArgs e)
        {
            foreach (var collection in collectionsCacheAside)
            {
                if (latestActivty.TryGetValue(new Tuple<string, string>(collection.Item1, collection.Item2), out var latestActivityForCollection))
                {
                    var databaseName = collection.Item1;
                    var collectioName = collection.Item2;
                    var minRu = collection.Item3;

                    if (DateTime.Now.AddMinutes(-5) > latestActivityForCollection)
                    {
                        //no activity for 5 minutes.. scale back down to minRu
                        ScaleLogic.ScaleDownCollectionAsync(_client, databaseName, collectioName, minRu).Wait();
                        Trace.WriteLine($"Inactivity longer then 5 minutes in {databaseName}|{collectioName}, scaling down to {minRu}RU.");
                    }
                }                
            }
        }

        #endregion
    }
}