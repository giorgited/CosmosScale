using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Timers;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace CosmosScale
{
    public class CosmosScaleOperator
    {
        private static DocumentClient _client;
        private int _minRu, _maxRu;
        private Uri _collectionUri;
        private string _databaseName, _collectionName;
        private string _collectionPartitionKey;
        private const int _maximumRetryCount = 30;


        private static System.Timers.Timer aTimer;
        private static Dictionary<Tuple<string,string>, Tuple<DateTime, int>> latestActivty = new Dictionary<Tuple<string, string>, Tuple<DateTime, int>>();
        private static bool timerSet = false;

        public CosmosScaleOperator(int minimumRu, int maximumRu, string databaseName, string collectionName, DocumentClient client, string partitionKeyPropertyName = "/id")
        {
            _client = client;
            _minRu = minimumRu;
            _maxRu = maximumRu;
            _databaseName = databaseName;
            _collectionName = collectionName;
            _collectionPartitionKey = partitionKeyPropertyName;
            _collectionUri = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName);

            latestActivty.Add(new Tuple<string, string>(_databaseName, _collectionName), new Tuple<DateTime,int>(DateTime.Now, _minRu));

            if (!timerSet)
            {
                SetTimer();
            }
        }
        

        public IEnumerable<T> QueryCosmos<T>(string cosmosSqlQuery, int maxItemCount = -1, bool QueryCrossPartition = true)
        {
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
                        var op = await ScaleLogic.ScaleUpCollectionAsync(_client, _databaseName, _collectionName, _minRu);
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
                        var op = await ScaleLogic.ScaleUpCollectionAsync(_client, _databaseName, _collectionName, _minRu);
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
        public async Task<CosmosOperationResponse> ReplaceDocument(string oldDocumentId, object newDocument)
        {
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
                        var op = await ScaleLogic.ScaleUpCollectionAsync(_client, _databaseName, _collectionName, _minRu);
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
                throw new Exception("databaseName or collectionName was not initialized.");
            }

            latestActivty[new Tuple<string, string>(_databaseName, _collectionName)] = new Tuple<DateTime, int>(DateTime.Now, _minRu);
        }
        private static void SetTimer()
        {
            // Create a timer with a 15 minute interval.
            aTimer = new Timer(TimeSpan.FromSeconds(10).TotalMilliseconds);
            aTimer.Elapsed += OnTimedEvent;
            aTimer.AutoReset = true;
            aTimer.Enabled = true;
        }
        private static void OnTimedEvent(Object source, ElapsedEventArgs e)
        {
            foreach (var activity in latestActivty)
            {
                var databaseName = activity.Key.Item1;
                var collectioName = activity.Key.Item2;

                var latestActivityDate = activity.Value.Item1;
                var minRu = activity.Value.Item2;

                if (DateTime.Now.AddMinutes(-1) > latestActivityDate)
                {
                    //no activity for 5 minutes.. scale back down to minRu
                    ScaleLogic.ScaleDownCollectionAsync(_client, databaseName, collectioName, minRu).Wait();
                }
            }
        }

        #endregion
    }
}