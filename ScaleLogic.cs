using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosScale
{
    public static class ScaleLogic
    {
        private static DateTimeOffset latestScaleUp = DateTime.MinValue;
        private static int currentRu = -1;

        public static async Task<ScaleOperation> ScaleUpCollectionAsync(DocumentClient _client, string _databaseName, string _collectionName, int minRu)
        {
            try
            {
                if (DateTime.Now.AddSeconds(-5) < latestScaleUp)
                {
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
                        Offer offer = _client.CreateOfferQuery()
                            .Where(r => r.ResourceLink == collection.SelfLink)
                            .AsEnumerable()
                            .SingleOrDefault();

                        if (currentRu == -1)
                        {
                            currentRu = minRu;
                        }

                        offer = new OfferV2(offer, (int)currentRu + 500);

                        await _client.ReplaceOfferAsync(offer);

                        latestScaleUp = DateTime.Now;
                        currentRu = currentRu + 500;

                        ScaleOperation op = new ScaleOperation();
                        op.ScaledFrom = (int)currentRu;
                        op.ScaledTo = (int)currentRu + 500;
                        op.OperationTime = DateTime.Now;
                        return op;
                    }
                }

                return new ScaleOperation()
                {
                    ScaledSuccess = false,
                    ScaleFailReason = "Could not find the collection to scale."
                };
            } catch (Exception e)
            {
                return new ScaleOperation()
                {
                    ScaledSuccess = false,
                    ScaleFailReason = e.Message
                };
            }
           
        }
    }


}
