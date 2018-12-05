using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosScale
{
    public static class ScaleLogic
    {
        //Tuple<string, string> --> DatabaseName, CollectionName
        private static ConcurrentDictionary<Tuple<string, string>, DateTime> latestScaleUp = new ConcurrentDictionary<Tuple<string, string>, DateTime>();
        private static object lockingObject = new object();

        public static async Task<ScaleOperation> ScaleUpCollectionAsync(DocumentClient _client, string _databaseName, string _collectionName, int minRu, int maxRu)
        {
            lock (lockingObject)
            {
                try
                {
                    var latestActivty = DateTime.MinValue;
                    if (latestScaleUp.TryGetValue(new Tuple<string, string>(_databaseName, _collectionName), out var res))
                    {
                        latestActivty = DateTime.Now;
                    }
                    if (DateTime.Now.AddSeconds(-5) < latestActivty)
                    {
                        Trace.WriteLine($"Tried to scale {_databaseName}|{_collectionName} but there has already been a scale in past 5 minutes.");
                        return new ScaleOperation()
                        {
                            ScaledSuccess = false,
                            ScaleFailReason = "There has been another scale within 5 seconds."
                        };
                    }

                    Database database = _client.CreateDatabaseQuery($"SELECT * FROM d WHERE d.id = \"{_databaseName}\"").AsEnumerable().First();

                    List<DocumentCollection> collections = _client.CreateDocumentCollectionQuery((String)database.SelfLink).ToList();

                    foreach (var collection in collections)
                    {
                        if (collection.Id == _collectionName)
                        {
                            var offer = (OfferV2)_client.CreateOfferQuery()
                                .Where(r => r.ResourceLink == collection.SelfLink)
                                .AsEnumerable()
                                .SingleOrDefault();

                            var currentRu = offer.Content.OfferThroughput;

                            if (currentRu + 500 <= maxRu)
                            {
                                offer = new OfferV2(offer, (int)currentRu + 500);

                                _client.ReplaceOfferAsync(offer).Wait();

                                latestScaleUp[new Tuple<string, string>(_databaseName, _collectionName)] = DateTime.Now;
                                currentRu = currentRu + 500;

                                ScaleOperation op = new ScaleOperation();
                                op.ScaledFrom = (int)currentRu;
                                op.ScaledTo = (int)currentRu + 500;
                                op.OperationTime = DateTime.Now;

                                Trace.WriteLine($"Sscaled {_databaseName}|{_collectionName} to {(int)currentRu + 500}");

                                return op;
                            }
                        }
                    }

                    return new ScaleOperation()
                    {
                        ScaledSuccess = false,
                        ScaleFailReason = "Could not find the collection to scale."
                    };
                }
                catch (Exception e)
                {
                    return new ScaleOperation()
                    {
                        ScaledSuccess = false,
                        ScaleFailReason = e.Message
                    };
                }
            }
        }

        public static async Task ScaleDownCollectionAsync(DocumentClient _client, string _databaseName, string _collectionName, int minRu)
        {
            Database database = _client.CreateDatabaseQuery($"SELECT * FROM d WHERE d.id = \"{_databaseName}\"").AsEnumerable().First();

            List<DocumentCollection> collections = _client.CreateDocumentCollectionQuery((String)database.SelfLink).ToList();

            foreach (var collection in collections)
            {
                if (collection.Id == _collectionName)
                {
                    Offer offer = _client.CreateOfferQuery()
                        .Where(r => r.ResourceLink == collection.SelfLink)
                        .AsEnumerable()
                        .SingleOrDefault();                       

                    offer = new OfferV2(offer, minRu);

                    await _client.ReplaceOfferAsync(offer);
                }
            }
        }
    }
}
