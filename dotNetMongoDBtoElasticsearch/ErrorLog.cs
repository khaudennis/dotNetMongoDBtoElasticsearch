using System.Collections.Generic;

namespace dotNetMongoDBtoElasticsearch
{
    class ErrorLog
    {
        public ErrorLog() { }
        public bool isError { get; set; }
        public int errorCount { get; set; }
        public List<Error> errorList { get; set; }
    }
}
