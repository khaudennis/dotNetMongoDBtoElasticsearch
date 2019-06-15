using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.ServiceProcess;

namespace dotNetMongoDBtoElasticsearch
{
    /// <summary>
    /// This program will connect to a MongoDB instance, pull incremental data from the MongoDB and perform a full index and/or delta index into an ElasticSearch instance via the Bulk API.
    /// </summary>
    class Program
    {
        #region Windows Service

        public readonly static string ServiceName = ConfigurationManager.AppSettings["ServiceName"].ToString();

        public class Service : ServiceBase
        {
            private static readonly int runtime = Convert.ToInt32(ConfigurationManager.AppSettings["ServiceRuntimeIntervalInMS"]);

            public Service()
            {
                ServiceName = Program.ServiceName;
            }

            protected override void OnStart(string[] args)
            {
                Program.Start(args);
            }

            protected override void OnStop()
            {
                Program.Stop();
            }
        }

        #endregion

        static readonly string mongoEnvironment = ConfigurationManager.AppSettings["MongoEnvironment"].ToString();
        static readonly bool isInteractive = false;
        static readonly Utilities u = new Utilities();
        static HttpClient _client;

        // ElasticSearch
        static readonly Uri esUri = new Uri(ConfigurationManager.ConnectionStrings["ElasticConnection"].ToString());
        static readonly string esAuthentication = ConfigurationManager.AppSettings["ELASTIC_AUTHENTICATION"].ToString();
        static readonly string esBulkContentType = ConfigurationManager.AppSettings["ELASTIC_BULK_CONTENT_TYPE"].ToString();
        static readonly string esBulkEndpoint = ConfigurationManager.AppSettings["ELASTIC_BULK_ENDPOINT"].ToString();

        // Indexing Configurations
        static readonly bool isReindex = Convert.ToBoolean(ConfigurationManager.AppSettings["IS_REINDEX"]);
        static readonly string indexPrefix = ConfigurationManager.AppSettings["ELASTIC_INDEX_PREFIX"].ToString();

        // MongoDB
        static readonly string mongoCollectionName = ConfigurationManager.AppSettings["MongoCollection"];
        static readonly string replicaSet = ConfigurationManager.AppSettings["MongoRS"].ToString();
        static readonly string useSSL = ConfigurationManager.AppSettings["MongoSSL"].ToString();
        static readonly string connectionString = ConfigurationManager.ConnectionStrings["MongoDBConnection"].ToString();
        static readonly string connectionStringRS = ConfigurationManager.ConnectionStrings["MongoDBConnection"].ToString() + "?replicaSet=" + replicaSet + "&ssl=" + useSSL;

        // Logs
        static readonly string logPath = ConfigurationManager.AppSettings["LogPath"].ToString();
        static readonly bool saveLastIndexToFS = Convert.ToBoolean(ConfigurationManager.AppSettings["LogLastIndexToFile"]);
        static readonly bool saveLogsToFS = Convert.ToBoolean(ConfigurationManager.AppSettings["LogToFile"]);
        static readonly string exceptionsIndex = ".exceptions-";
        static readonly string errorIndex = ".errors-";

        static string argDatabase = null;
        static string argCollection = null;
        static string argDocType = null;
        static string argId = null;

        static void Main(string[] args)
        {
            Program.Start(args);
        }

        /// <summary>
        /// Starts the program.
        /// </summary>
        /// <param name="args"></param>
        private static void Start(string[] args)
        {
            try
            {
                Console.WriteLine("+++ MongoDB to ElasticSearch Data Sync +++\n");
                Console.WriteLine("Using MongoDB at " + ((mongoEnvironment.ToLower() == "development") ? connectionString.ToUpper() : connectionStringRS.ToUpper()));
                Console.WriteLine("Sending to ES at " + esUri.AbsoluteUri.ToUpper() + "\n");
                _client = new HttpClient { BaseAddress = esUri };
                _client.DefaultRequestHeaders.Accept.Clear();
                _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue(esBulkContentType));

                var byteArray = Encoding.ASCII.GetBytes(esAuthentication);
                _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));

                string[] collections = mongoCollectionName.Split(",".ToCharArray());

                if (args.Length > 0)
                {
                    var arp = string.Join(",", args).Split(new char[0]);

                    for (int i = 0; i < arp.Length; i++)
                    {
                        if (arp[i] == "--db") { argDatabase = arp[i + 1].ToString(); }
                        if (arp[i] == "--c") { argCollection = arp[i + 1].ToString(); }
                        if (arp[i] == "--type") { argDocType = arp[i + 1].ToString(); }
                        if (arp[i] == "--id") { argId = arp[i + 1].ToString(); }
                    }

                    if (arp.Length == 8) { LoadSingleDoc(argCollection, argDocType, argId).Wait(); }
                }
                else
                {
                    if (isReindex)
                    {
                        foreach (var c in collections) { LoadBulkData(c.ToString()).Wait(); }
                    }
                    else
                    {
                        foreach (var c in collections)
                        {
                            List<LastIndex> logs = new List<LastIndex>();

                            if (saveLastIndexToFS)
                            {
                                logs = ReadLastIndex(logPath + "LastIndex.txt", (indexPrefix + c).ToLower());
                            }
                            else
                            {
                                Task.Run(async () => { logs = await ReadLastIndex((indexPrefix + c).ToLower()); }).GetAwaiter().GetResult();
                            }

                            if (logs.Count == 0)
                            {
                                LoadBulkData(c.ToString()).Wait();
                            }
                            else
                            {
                                var lastKnownIndexId = logs.OrderByDescending(x => x.IndexDateTime).First().LastIndexId;

                                if (string.IsNullOrEmpty(lastKnownIndexId))
                                {
                                    LoadBulkData(c.ToString()).Wait();
                                }
                                else
                                {
                                    DeltaBulkData(c, lastKnownIndexId).Wait();
                                }
                            }
                        }
                    }
                }

                if (isInteractive)
                {
                    Console.WriteLine("\nThe batch load process has completed, press [ENTER] to exit...\n");
                    Console.ReadKey(true);
                }

                Stop();
            }
            catch (Exception e)
            {
                Console.Write("\n\n+++ GENERAL EXCEPTION +++\n\n" + e.Message.ToString() + "\n\n");

                if (saveLogsToFS)
                {
                    u.SaveToFile(logPath + "Exceptions_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", "Main()\t" + e.Message.ToString() + " (" + e.InnerException.Message.ToString() + ")");
                }
                else { SaveExceptionToElastic("Main()", e).Wait(); }

                if (isInteractive) { Console.ReadKey(true); }
                Stop();
            }
        }

        /// <summary>
        /// Stops the program.
        /// </summary>
        /// <param name="args"></param>
        private static void Stop()
        {
            Environment.Exit(0);
        }

        #region Program Methods

        /// <summary>
        /// Connects to MongoDB and incrementally pulls data to be parsed and bulk loaded into the ElasticSearch instance via the Bulk API.  This method performs a full reindex.
        /// </summary>
        /// <param name="collectionName">string</param>
        /// <returns></returns>
        private static async Task LoadBulkData(string collectionName)
        {
            try
            {
                MongoConnectionSettings myMongo = new MongoConnectionSettings
                {
                    limit = Convert.ToInt32(ConfigurationManager.AppSettings["MongoCount"]),
                    skipCount = Convert.ToInt32(ConfigurationManager.AppSettings["SkipCount"]),
                    databaseName = ConfigurationManager.AppSettings["MongoDatabase"],
                    collectionName = collectionName
                };

                var limit = myMongo.limit;
                var skipCount = myMongo.skipCount;

                var Client = new MongoClient((mongoEnvironment.ToLower() == "development") ? connectionString : connectionStringRS);
                var DB = Client.GetDatabase(myMongo.databaseName);
                var collection = DB.GetCollection<BsonDocument>(myMongo.collectionName);
                var mongoCount = collection.CountDocuments(new BsonDocument());

                Console.Write("{0} Records Acquired from MongoDB in {1}\n", mongoCount, collectionName);

                var indexName = indexPrefix + myMongo.collectionName.ToLower();
                var documentType = myMongo.collectionName.ToLower();

                StringBuilder bulk = new StringBuilder();

                for (int i = 0; i < mongoCount; i += limit)
                {
                    bulk.Clear();

                    var results = await collection.Find(new BsonDocument())
                           .Sort(Builders<BsonDocument>.Sort.Ascending("_id"))
                           .Limit(limit)
                           .Skip(skipCount)
                           .ToListAsync();

                    skipCount += limit;
                    string lastIndexId = results[results.Count - 1]["_id"].ToString();

                    foreach (var doc in results)
                    {
                        var documentId = doc["_id"].ToString();
                        doc.Remove("_id");

                        string metadata = "{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_type\" : \"" + doc["type"].ToString().ToLower() + "\", \"_id\" : \"" + documentId + "\" } } \n";

                        bulk.Append(metadata);
                        bulk.Append(doc.ToJson(new JsonWriterSettings { OutputMode = JsonOutputMode.Strict }) + "\n");
                    }

                    var bodyLength = bulk.Length;
                    var bulkJson = bulk;

                    ErrorLog el = await SendToElasticBulkApi(bulkJson);

                    if (el.isError)
                    {
                        int newResultsCount = results.Count - el.errorCount;
                        Console.Write("\n+++ STATS +++\n");
                        Console.Write("\nElasticSearch responded with errors, please review logs for more information.\n");
                        Console.Write("{0} Errors\n", el.errorCount.ToString());
                        Console.Write("{0} Record(s) Loaded in this Batch\n", newResultsCount.ToString());
                    }
                    else
                    {
                        Console.Write("\n{0} Records Loaded in this Batch\n", results.Count.ToString());
                    }

                    if (saveLogsToFS)
                    {
                        if (saveLastIndexToFS) u.SaveToFile(logPath + "LastIndex.txt", indexName + "\t" + lastIndexId + "\t" + (results.Count - el.errorCount).ToString() + "\t" + el.errorCount.ToString() + "\t" + "BULK REINDEX");
                        u.SaveToFile(logPath + "LastIndex_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", indexName + "\t" + lastIndexId + "\t" + (results.Count - el.errorCount).ToString() + "\t" + el.errorCount.ToString() + "\t" + "BULK REINDEX");
                    }
                    else
                    {
                        await SaveLogToElastic("{ \"indexDateTime\" : \"" + DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "\", \"indexName\" : \"" + indexName + "\", \"lastIndexId\" : \"" + lastIndexId + "\", \"lastSuccessCount\" : \"" + (results.Count - el.errorCount).ToString() + "\", \"lastFailCount\" : \"" + el.errorCount.ToString() + "\", \"loadType\" : \"BULK REINDEX\"  }\n");
                    }
                }
            }
            catch (Exception e)
            {
                if (saveLogsToFS)
                {
                    u.SaveToFile(logPath + "Exceptions_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", "LoadBulkData()\t" + e.Message.ToString() + " (" + e.InnerException.Message.ToString() + ")");
                }
                else
                {
                    SaveExceptionToElastic("LoadBulkData()", e).Wait();
                }
            }
        }

        /// <summary>
        /// Connects to MongoDB and incrementally pulls data to be parsed and bulk loaded into the ElasticSearch instance via the Bulk API. This method only performs a delta update.
        /// </summary>
        /// <param name="collectionName">string</param>
        /// <param name="lastKnownIndexId">string</param>
        /// <returns></returns>
        private static async Task DeltaBulkData(string collectionName, string lastKnownIndexId)
        {
            try
            {
                MongoConnectionSettings myMongo = new MongoConnectionSettings
                {
                    limit = Convert.ToInt32(ConfigurationManager.AppSettings["MongoCount"]),
                    skipCount = Convert.ToInt32(ConfigurationManager.AppSettings["SkipCount"]),
                    databaseName = ConfigurationManager.AppSettings["MongoDatabase"],
                    collectionName = collectionName
                };

                var limit = myMongo.limit;
                var skipCount = myMongo.skipCount;

                var Client = new MongoClient((mongoEnvironment.ToLower() == "development") ? connectionString : connectionStringRS);
                var DB = Client.GetDatabase(myMongo.databaseName);
                var collection = DB.GetCollection<BsonDocument>(myMongo.collectionName);
                var mongoCount = collection.CountDocuments(new BsonDocument());

                Console.Write("{0} Records Acquired from MongoDB in {1}\n", mongoCount, collectionName);

                var indexName = indexPrefix + myMongo.collectionName.ToLower();

                bool isDeltaFound = false;
                StringBuilder bulk = new StringBuilder();

                for (int i = 0; i < mongoCount; i += limit)
                {
                    bulk.Clear();

                    var filter = Builders<BsonDocument>.Filter.Gt(x => x["_id"], lastKnownIndexId);
                    var results = await collection.Find(filter)
                           .Sort(Builders<BsonDocument>.Sort.Ascending("_id"))
                           .Limit(limit)
                           .Skip(skipCount)
                           .ToListAsync();

                    if (results.Count == 0 && isDeltaFound == false) { continue; }
                    else if (results.Count >= 1 && isDeltaFound == false) { isDeltaFound = true; }
                    else if (results.Count == 0 && isDeltaFound) { continue; ; }

                    skipCount += limit;
                    string lastIndexId = results[results.Count - 1]["_id"].ToString();

                    foreach (var doc in results)
                    {
                        var documentId = doc["_id"].ToString();
                        doc.Remove("_id");

                        string metadata = "{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_type\" : \"" + doc["type"].ToString().ToLower() + "\", \"_id\" : \"" + documentId + "\" } } \n";

                        bulk.Append(metadata);
                        bulk.Append(doc.ToJson(new JsonWriterSettings { OutputMode = JsonOutputMode.Strict }) + "\n");
                    }

                    var bodyLength = bulk.Length;
                    var bulkJson = bulk;

                    ErrorLog el = await SendToElasticBulkApi(bulkJson);

                    if (el.isError)
                    {
                        int newResultsCount = results.Count - el.errorCount;
                        Console.Write("\n+++ STATS +++\n");
                        Console.Write("\nElasticSearch responded with errors, please review logs for more information.\n");
                        Console.Write("{0} Errors\n", el.errorCount.ToString());
                        Console.Write("{0} Record(s) Loaded in this Batch\n", newResultsCount.ToString());
                    }
                    else
                    {
                        Console.Write("\n{0} Records Loaded in this Batch\n", results.Count.ToString());
                    }

                    if (saveLogsToFS)
                    {
                        if (saveLastIndexToFS) u.SaveToFile(logPath + "LastIndex.txt", indexName + "\t" + lastIndexId + "\t" + (results.Count - el.errorCount).ToString() + "\t" + el.errorCount.ToString() + "\t" + "BULK DELTA");
                        u.SaveToFile(logPath + "LastIndex_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", indexName + "\t" + lastIndexId + "\t" + (results.Count - el.errorCount).ToString() + "\t" + el.errorCount.ToString() + "\t" + "BULK DELTA");
                    }
                    else
                    {
                        await SaveLogToElastic("{ \"indexDateTime\" : \"" + DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "\", \"indexName\" : \"" + indexName + "\", \"lastIndexId\" : \"" + lastIndexId + "\", \"lastSuccessCount\" : \"" + (results.Count - el.errorCount).ToString() + "\", \"lastFailCount\" : \"" + el.errorCount.ToString() + "\", \"loadType\" : \"BULK DELTA\"  }\n");
                    }
                }

                if (!isDeltaFound) { Console.Write(">> No delta records found in {0}\n\n", collectionName); } else Console.Write("\n\n");
            }
            catch (Exception e)
            {
                if (saveLogsToFS)
                {
                    u.SaveToFile(logPath + "Exceptions_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", "DeltaBulkData()\t" + e.Message.ToString() + " (" + e.InnerException.Message.ToString() + ")");
                }
                else { SaveExceptionToElastic("DeltaBulkData()", e).Wait(); }
            }
        }

        private static async Task LoadSingleDoc(string collectionName, string docType, string indexId)
        {
            try
            {
                MongoConnectionSettings myMongo = new MongoConnectionSettings
                {
                    limit = Convert.ToInt32(ConfigurationManager.AppSettings["MongoCount"]),
                    skipCount = Convert.ToInt32(ConfigurationManager.AppSettings["SkipCount"]),
                    databaseName = ConfigurationManager.AppSettings["MongoDatabase"],
                    collectionName = collectionName
                };

                var Client = new MongoClient((mongoEnvironment.ToLower() == "development") ? connectionString : connectionStringRS);
                var DB = Client.GetDatabase(myMongo.databaseName);
                var collection = DB.GetCollection<BsonDocument>(myMongo.collectionName);
                var mongoCount = collection.CountDocuments(new BsonDocument());

                Console.Write("{0} Records Acquired from MongoDB in {1}\n", mongoCount, collectionName);

                var indexName = indexPrefix + myMongo.collectionName.ToLower();
                var documentType = myMongo.collectionName.ToLower();

                StringBuilder bulk = new StringBuilder();
                bulk.Clear();

                var results = await collection.Find(x => x["type"] == docType && x["_id"] == indexId).ToListAsync();

                foreach (var doc in results)
                {
                    var documentId = doc["_id"].ToString();
                    doc.Remove("_id");

                    string metadata = "{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_type\" : \"" + docType.ToLower() + "\", \"_id\" : \"" + documentId + "\" } } \n";

                    bulk.Append(metadata);
                    bulk.Append(doc.ToJson(new JsonWriterSettings { OutputMode = JsonOutputMode.Strict }) + "\n");
                }

                ErrorLog el = await SendToElasticBulkApi(bulk);

                if (el.isError)
                {
                    int newResultsCount = results.Count - el.errorCount;
                    Console.Write("\n+++ STATS +++\n");
                    Console.Write("\nElasticSearch responded with errors, please review logs for more information.\n");
                    Console.Write("{0} Errors\n", el.errorCount.ToString());
                    Console.Write("{0} Record(s) Loaded in this Batch\n", newResultsCount.ToString());
                }
                else
                {
                    Console.Write("\n{0} Records Loaded in this Batch\n", results.Count.ToString());
                }

                if (saveLogsToFS)
                {
                    if (saveLastIndexToFS) u.SaveToFile(logPath + "LastIndex.txt", indexName + "\t" + indexId + "\t" + (results.Count - el.errorCount).ToString() + "\t" + el.errorCount.ToString() + "\t" + "SINGLE REINDEX");
                    u.SaveToFile(logPath + "LastIndex_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", indexName + "\t" + indexId + "\t" + (results.Count - el.errorCount).ToString() + "\t" + el.errorCount.ToString() + "\t" + "SINGLE REINDEX");
                }
                else
                {
                    await SaveLogToElastic("{ \"indexDateTime\" : \"" + DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "\", \"indexName\" : \"" + indexName + "\", \"lastIndexId\" : \"" + indexId + "\", \"lastSuccessCount\" : \"" + (results.Count - el.errorCount).ToString() + "\", \"lastFailCount\" : \"" + el.errorCount.ToString() + "\", \"loadType\" : \"SINGLE REINDEX\" }\n");
                }
            }
            catch (Exception e)
            {
                if (saveLogsToFS)
                {
                    u.SaveToFile(logPath + "Exceptions_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", "LoadSingleDoc()\t" + e.Message.ToString() + " (" + e.InnerException.Message.ToString() + ")");
                }
                else
                {
                    SaveExceptionToElastic("LoadSingleDoc()", e).Wait();
                }
            }
        }

        /// <summary>
        /// Performs a HTTP GET to an ElasticSearch instance to check if an index exists.
        /// </summary>
        /// <param name="indexName">string</param>
        /// <returns></returns>
        private static async Task<bool> CheckIndexExists(string indexName)
        {
            try
            {
                var response = await _client.GetAsync(indexName);
                if (response.StatusCode == System.Net.HttpStatusCode.OK) { return true; } else { return false; }
            }
            catch (Exception e)
            {
                if (saveLogsToFS)
                {
                    u.SaveToFile(logPath + "Exceptions_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", "CheckIndexExists()\t" + e.Message.ToString());
                }
                else { SaveExceptionToElastic("CheckIndexExists()", e).Wait(); }

                return false;
            }
        }

        /// <summary>
        /// Performs a HTTP GET to an ElasticSearch instance and provide the count of documents in its index.
        /// </summary>
        /// <param name="indexName">string</param>
        /// <returns></returns>
        private static async Task<int> CheckIndexCount(string indexName)
        {
            try
            {
                var response = await _client.GetAsync(indexName + "/_count/");
                var content = await response.Content.ReadAsStringAsync();
                JObject o = JObject.Parse(content);
                if ((int)o["count"] > 0) { return (int)o["count"]; } else { return -1; }
            }
            catch (Exception e)
            {
                if (saveLogsToFS)
                {
                    u.SaveToFile(logPath + "Exceptions_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", "CheckIndexCount()\t" + e.Message.ToString());
                }
                else { SaveExceptionToElastic("CheckIndexCount()", e).Wait(); }

                return -1;
            }
        }

        /// <summary>
        /// Performs a HTTP POST to an ElasticSearch instance via the Bulk API and provides its response.
        /// </summary>
        /// <param name="bulkJson">StringBuilder</param>
        /// <returns></returns>
        private static async Task<ErrorLog> SendToElasticBulkApi(StringBuilder bulkJson)
        {
            try
            {
                //Console.Write("\n\n+++ BODY +++\n\n" + bulkJson.ToString().Substring(0, 250) + " \n...\n " + bulkJson.ToString().Substring(bulkJson.Length - 250, 250) + "\n\n");

                HttpContent _body = new StringContent(bulkJson.ToString(), Encoding.UTF8, esBulkContentType);
                _body.Headers.ContentType = new MediaTypeHeaderValue(esBulkContentType);

                var response = await _client.PostAsync(esBulkEndpoint, _body);
                //response.EnsureSuccessStatusCode();

                var content = await response.Content.ReadAsStringAsync();
                //Console.Write(content + "\n");

                return ProcessResponse(content);
            }
            catch (Exception e)
            {
                if (saveLogsToFS)
                {
                    u.SaveToFile(logPath + "Exceptions_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", "SendToElasticBulkApi()\t" + e.Message.ToString());
                }
                else { SaveExceptionToElastic("SendToElasticBulkApi()", e).Wait(); }

                return null;
            }
        }

        /// <summary>
        /// Performs a HTTP GET to an ElasticSearch instance and provide its response.
        /// </summary>
        /// <param name="esEndpoint">string</param>
        /// <returns></returns>
        private static async Task<dynamic> GetFromElastic(string esEndpoint)
        {
            try
            {
                var response = await _client.GetAsync(esEndpoint);
                var content = await response.Content.ReadAsStringAsync();
                //INFO:  Show response from ElasticSearch...
                //Console.Write(content + "\n");

                ErrorLog err = ProcessResponse(content);
                if (err.isError == false) { return content; } else { return err; }
            }
            catch (Exception e)
            {
                if (saveLogsToFS)
                {
                    u.SaveToFile(logPath + "Exceptions_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", "GetFromElastic()\t" + e.Message.ToString());
                }
                else { SaveExceptionToElastic("GetFromElastic()", e).Wait(); }

                return null;
            }
        }

        /// <summary>
        /// Processes the error response, if applicable, from the ElasticSearch instance response.
        /// </summary>
        /// <param name="elasticResponse">dynamic</param>
        /// <returns></returns>
        private static ErrorLog ProcessResponse(dynamic elasticResponse)
        {
            try
            {
                JObject o = JObject.Parse(elasticResponse);
                ErrorLog el = new ErrorLog();

                if (o["errors"] != null)
                {
                    bool isError = (bool)o["errors"];

                    el.errorList = new List<Error>();
                    el.isError = false;

                    if (isError)
                    {
                        el.isError = true;
                        var errors = o["items"];

                        foreach (var e in errors)
                        {
                            int errorCode = Convert.ToInt32(e["index"]["status"].ToString());
                            Error err = new Error();
                            if (errorCode == 200 || errorCode == 201) { continue; }

                            err.indexName = e["index"]["_index"].ToString();
                            err.documentType = e["index"]["_type"].ToString();
                            err.documentId = e["index"]["_id"].ToString();
                            err.code = errorCode.ToString();
                            err.reason = e["index"]["error"]["reason"].ToString();

                            var errorLine = err.indexName + "\t" + err.documentId + "\t" + err.documentType + "\t" + err.code + "\t" + err.reason;

                            el.errorCount += 1;
                            el.errorList.Add(err);

                            if (saveLogsToFS)
                            {
                                u.SaveToFile(logPath + "Errors_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", errorLine);
                            }
                            else
                            {
                                string logData = "{ \"indexName\" : \"" + err.indexName + "\", \"documentType\" : \"" + err.documentType + "\", \"documentId\" : \"" + err.documentId + "\", \"errorCode\" : \"" + err.code + "\", \"errorReason\" : \"" + err.reason + "\" }\n";
                                SaveLogToElastic(logData, errorIndex + DateTime.Today.ToString("yyyy-MM-dd"), "error").Wait();
                            }
                        }

                        return el;
                    }
                    else { return el; }
                }
                else {
                    el = new ErrorLog
                    {
                        isError = false
                    }; return el; }
            }
            catch (Exception e)
            {
                if (saveLogsToFS)
                {
                    u.SaveToFile(logPath + "Exceptions_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", "ProcessResponse()\t" + e.Message.ToString());
                }
                else { SaveExceptionToElastic("ProcessResponse()", e).Wait(); }

                return null;
            }
        }

        /// <summary>
        /// Gets the lastIndexId for an index via the file system.
        /// </summary>
        /// <param name="filePath">string</param>
        /// <param name="indexName">string</param>
        /// <returns></returns>
        private static List<LastIndex> ReadLastIndex(string filePath, string indexName)
        {
            try
            {
                List<LastIndex> indexLog = new List<LastIndex>();

                if (System.IO.File.Exists(filePath))
                {
                    System.IO.StreamReader sr = new System.IO.StreamReader(filePath);
                    string line;

                    while ((line = sr.ReadLine()) != null)
                    {
                        LastIndex l = new LastIndex();
                        string[] logLineItems = line.Split(new char[] { '\t' }, StringSplitOptions.None);

                        if (logLineItems[1].ToLower() == indexName.ToLower())
                        {
                            l.IndexDateTime = Convert.ToDateTime(logLineItems[0]);
                            l.IndexName = logLineItems[1];
                            l.LastIndexId = logLineItems[2];
                            l.LoadSuccessCount = Convert.ToInt32(logLineItems[3]);
                            l.LoadFailCount = Convert.ToInt32(logLineItems[4]);
                            l.LoadType = logLineItems[5];

                            indexLog.Add(l);
                        }
                    }
                }

                return indexLog;
            }
            catch (Exception e)
            {
                if (saveLogsToFS)
                {
                    u.SaveToFile(logPath + "Exceptions_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", "ReadLastIndex()\t" + e.Message.ToString());
                }
                else { SaveExceptionToElastic("ReadLastIndex()", e).Wait(); }

                return null;
            }
        }

        /// <summary>
        /// Gets the lastIndexId for an index via the ElasticSearch instance.
        /// </summary>
        /// <param name="indexName">string</param>
        /// <returns></returns>
        private static async Task<List<LastIndex>> ReadLastIndex(string indexName)
        {
            try
            {
                string logIndex = "lastindices";
                bool isLogExist = await CheckIndexExists(logIndex);

                if (isLogExist)
                {
                    var content = GetFromElastic(logIndex + "/");
                    dynamic esResponse = await GetFromElastic(logIndex + "/_search?size=100");
                    var t = esResponse.GetType();

                    if (t.Equals(typeof(string)))
                    {
                        JObject o = JObject.Parse(esResponse);
                        LastIndex li = new LastIndex();
                        var documents = o["hits"]["hits"];
                        List<LastIndex> indices = new List<LastIndex>();

                        foreach (var index in documents)
                        {
                            if (index["_source"]["indexName"].ToString().ToLower() == indexName.ToLower())
                            {
                                li = new LastIndex
                                {
                                    Id = (Guid)index["_id"],
                                    IndexDateTime = (DateTime)index["_source"]["indexDateTime"],
                                    IndexName = index["_source"]["indexName"].ToString(),
                                    LastIndexId = index["_source"]["lastIndexId"].ToString(),
                                    LoadSuccessCount = (int)index["_source"]["lastSuccessCount"],
                                    LoadFailCount = (int)index["_source"]["lastFailCount"],
                                    LoadType = index["_source"]["loadType"].ToString()
                                };
                                indices.Add(li);
                            }
                            else { continue; }
                        }

                        if (indices.Count == 0 || indices == null)
                        {
                            return new List<LastIndex>();
                        }
                        else
                        {
                            List<LastIndex> refinedIndices = await CleanLastIndexLog(indices);
                            return refinedIndices;
                        }
                    }
                    else if (t.Equals(typeof(ErrorLog)))
                    {
                        new Exception("ElasticSearch returned an error on querying lastIndexId from \"" + logIndex + "\".");
                    }
                }

                return new List<LastIndex>();
            }
            catch (Exception e)
            {
                if (saveLogsToFS)
                {
                    u.SaveToFile(logPath + "Exceptions_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", "ReadLastIndex()\t" + e.Message.ToString());
                }
                else { SaveExceptionToElastic("ReadLastIndex()", e).Wait(); }

                return new List<LastIndex>();
            }
        }

        /// <summary>
        /// Saves the lastIndex to the ElasticSearch instance via the Bulk API.
        /// </summary>
        /// <param name="logData">string</param>
        /// <returns></returns>
        private static async Task<bool> SaveLogToElastic(string logData)
        {
            try
            {
                StringBuilder logBulk = new StringBuilder();
                string logMetadata = "{ \"index\" : { \"_index\" : \"lastindices\", \"_type\" : \"lastindex\", \"_id\" : \"" + Guid.NewGuid().ToString() + "\" } } \n";
                logBulk.Append(logMetadata);
                logBulk.Append(logData);
                ErrorLog logEl = await SendToElasticBulkApi(logBulk);
                if (logEl.errorCount == 0) { return true; } else { return false; }
            }
            catch (Exception e)
            {
                if (saveLogsToFS)
                {
                    u.SaveToFile(logPath + "Exceptions_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", "SaveLogToElastic()\t" + e.Message.ToString());
                }
                else { SaveExceptionToElastic("SaveLogToElastic()", e).Wait(); }

                return false;
            }
        }

        /// <summary>
        /// Saves logs to the ElasticSearch instance via the Bulk API.
        /// </summary>
        /// <param name="logData">string</param>
        /// <param name="indexName">string</param>
        /// <param name="indexType">string</param>
        /// <returns></returns>
        private static async Task<bool> SaveLogToElastic(string logData, string indexName, string indexType)
        {
            try
            {
                StringBuilder logBulk = new StringBuilder();
                string logMetadata = "{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_type\" : \"" + indexType + "\", \"_id\" : \"" + Guid.NewGuid().ToString() + "\" } } \n";
                logBulk.Append(logMetadata);
                logBulk.Append(logData);
                ErrorLog logEl = await SendToElasticBulkApi(logBulk);
                if (logEl.errorCount == 0) { return true; } else { return false; }
            }
            catch (Exception e)
            {
                if (saveLogsToFS)
                {
                    u.SaveToFile(logPath + "Exceptions_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", "SaveLogToElastic()\t" + e.Message.ToString());
                }
                else { SaveExceptionToElastic("SaveLogToElastic()", e).Wait(); }
                return false;
            }
        }

        /// <summary>
        /// Removes old lastIndexIds and keeping the most recent in ElasticSearch.
        /// </summary>
        /// <param name="indices">List<LastIndex></param>
        /// <returns></returns>
        private static async Task<List<LastIndex>> CleanLastIndexLog(List<LastIndex> indices)
        {
            try
            {
                LastIndex keepLastIndex = indices.OrderByDescending(x => x.IndexDateTime).First();

                foreach (LastIndex index in indices)
                {
                    if (index.Id == keepLastIndex.Id) { continue; }
                    var response = await _client.DeleteAsync("lastindices/lastindex/" + index.Id.ToString());
                    if (response.IsSuccessStatusCode) { continue; }
                }

                indices.RemoveAll(x => x.Id != keepLastIndex.Id);

                return indices;
            }
            catch (Exception e)
            {
                if (saveLogsToFS)
                {
                    u.SaveToFile(logPath + "Exceptions_" + DateTime.Today.ToString("yyyyMMdd") + ".txt", "CleanLastIndexLog()\t" + e.Message.ToString());
                }
                else { SaveExceptionToElastic("CleanLastIndexLog()", e).Wait(); }

                return null;
            }
        }

        //TODO:  Move to another class...
        private static async Task<bool> SaveExceptionToElastic(string processHandler, Exception e)
        {
            string exceptionData = "{ \"exceptionDateTime\" : \"" + DateTime.Today.ToString("yyyy-MM-dd hh:mm:ss") + "\", \"process\" : \"" + processHandler + "\", \"exceptionMessage\" : \"" + e.Message.ToString() + "\", \"innerExceptionMessage\" : \"" + (!String.IsNullOrEmpty(e.InnerException.Message) ? e.InnerException.Message.ToString() : "null") + "\", \"exceptionStackTrace\" : \"" + e.StackTrace.ToString() + "\" }\n";
            return await SaveLogToElastic(exceptionData, exceptionsIndex + DateTime.Today.ToString("yyyyMMdd"), "exception");
        }

        #endregion
    }
}