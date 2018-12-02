using System;
using System.Collections.Generic;
using System.Text;

namespace CosmosScale
{
    public class CosmosOperationResponse
    {
        public bool Success { get; set; }
        public List<ScaleOperation> ScaleOperations { get; set; } = new List<ScaleOperation>();
        public int TotalRetries { get; set; }
    }
}
