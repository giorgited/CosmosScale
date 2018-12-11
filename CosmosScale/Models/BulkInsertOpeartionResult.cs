using System;
using System.Collections.Generic;
using System.Text;

namespace CosmosScale.Models
{
    public class BulkInsertOpeartionResult
    {
        public bool OperationSuccess { get; set; }
        public List<ScaleOperation> ScaleOperations { get; set; } = new List<ScaleOperation>();
        public string OperationFailReason { get; set; }
    }
}
