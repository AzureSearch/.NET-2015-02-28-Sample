using Microsoft.Azure;
using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using Microsoft.Spatial;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace DataIndexer
{
    class Program
    {
        private static SearchServiceClient _searchClient;
        private static SearchIndexClient _indexClient;
        private static string _fileName = @"USGS_WA_Features_Descriptions.txt";

        // This Sample shows how to delete, create, upload documents and query an index
        static void Main(string[] args)
        {
            string searchServiceName = ConfigurationManager.AppSettings["SearchServiceName"];
            string apiKey = ConfigurationManager.AppSettings["SearchServiceApiKey"];

            // Create an HTTP reference to the catalog index
            _searchClient = new SearchServiceClient(searchServiceName, new SearchCredentials(apiKey));
            _indexClient = _searchClient.Indexes.GetClient("features");

            Console.WriteLine("{0}", "Deleting index...\n");
            if (DeleteIndex())
            {
                Console.WriteLine("{0}", "Creating index...\n");
                CreateIndex();
                Console.WriteLine("{0}", "Uploading documents...\n");
                UploadDocuments();
            }
            Console.WriteLine("{0}", "Complete.  Press any key to end application...\n");
            Console.ReadKey();
        }

        private static bool DeleteIndex()
        {
            // Delete the index if it exists
            try
            {
                AzureOperationResponse response = _searchClient.Indexes.Delete("features");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error deleting index: {0}\r\n", ex.Message.ToString());
                Console.WriteLine("Did you remember to add your SearchServiceName and SearchServiceApiKey to the app.config?\r\n");
                return false;
            }

            return true;
        }

        private static void CreateIndex()
        {
            // Create the Azure Search index based on the included schema
            try
            {
                var definition = new Index()
                {
                    Name = "features",
                    Fields = new[] 
                    { 
                        new Field("FEATURE_ID",     DataType.String)         { IsKey = true,  IsSearchable = false, IsFilterable = false, IsSortable = false, IsFacetable = false, IsRetrievable = true},
                        new Field("FEATURE_NAME",   DataType.String)         { IsKey = false, IsSearchable = true,  IsFilterable = true,  IsSortable = true,  IsFacetable = false, IsRetrievable = true},
                        new Field("FEATURE_CLASS",  DataType.String)         { IsKey = false, IsSearchable = true,  IsFilterable = true,  IsSortable = true,  IsFacetable = false, IsRetrievable = true},
                        new Field("STATE_ALPHA",    DataType.String)         { IsKey = false, IsSearchable = true,  IsFilterable = true,  IsSortable = true,  IsFacetable = false, IsRetrievable = true},
                        new Field("STATE_NUMERIC",  DataType.Int32)          { IsKey = false, IsSearchable = false, IsFilterable = true,  IsSortable = true,  IsFacetable = true,  IsRetrievable = true},
                        new Field("COUNTY_NAME",    DataType.String)         { IsKey = false, IsSearchable = true,  IsFilterable = true,  IsSortable = true,  IsFacetable = false, IsRetrievable = true},
                        new Field("COUNTY_NUMERIC", DataType.Int32)          { IsKey = false, IsSearchable = false, IsFilterable = true,  IsSortable = true,  IsFacetable = true,  IsRetrievable = true},
                        new Field("LOCATION",       DataType.GeographyPoint) { IsKey = false, IsSearchable = false, IsFilterable = true,  IsSortable = true,  IsFacetable = false, IsRetrievable = true},
                        new Field("ELEV_IN_M",      DataType.Int32)          { IsKey = false, IsSearchable = false, IsFilterable = true,  IsSortable = true,  IsFacetable = true,  IsRetrievable = true},
                        new Field("ELEV_IN_FT",     DataType.Int32)          { IsKey = false, IsSearchable = false, IsFilterable = true,  IsSortable = true,  IsFacetable = true,  IsRetrievable = true},
                        new Field("MAP_NAME",       DataType.String)         { IsKey = false, IsSearchable = true,  IsFilterable = true,  IsSortable = true,  IsFacetable = false, IsRetrievable = true},
                        new Field("DESCRIPTION",    DataType.String)         { IsKey = false, IsSearchable = true,  IsFilterable = false, IsSortable = false, IsFacetable = false, IsRetrievable = true},
                        new Field("HISTORY",        DataType.String)         { IsKey = false, IsSearchable = true,  IsFilterable = false, IsSortable = false, IsFacetable = false, IsRetrievable = true},
                        new Field("DATE_CREATED",   DataType.DateTimeOffset) { IsKey = false, IsSearchable = false, IsFilterable = true,  IsSortable = true,  IsFacetable = true,  IsRetrievable = true},
                        new Field("DATE_EDITED",    DataType.DateTimeOffset) { IsKey = false, IsSearchable = false, IsFilterable = true,  IsSortable = true,  IsFacetable = true,  IsRetrievable = true}
                    }
                };

                _searchClient.Indexes.Create(definition);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error creating index: {0}\r\n", ex.Message.ToString());
            }

        }

        private static void UploadDocuments()
        {
            //Load a CSV file and upload it as a batch of documents
            System.Data.DataTable dt = LoadCSV(_fileName);

            List<IndexAction> indexOperations = new List<IndexAction>();
            int colCounter = 0, rowCounter = 0, outInt;
            double PRIM_LAT_DEC=0, PRIM_LONG_DEC=0;

            // Skip the first header row
            for (int i = 1; i < dt.Rows.Count; i++)
            {
                DataRow dtRow = dt.Rows[i];
                IndexAction ia = new IndexAction();
                Document doc = new Document();
                colCounter = 0;
                foreach (DataColumn dc in dt.Columns)
                {
                    if (dt.Columns[colCounter].ToString() == "PRIM_LAT_DEC")
                        PRIM_LAT_DEC = Convert.ToDouble(dtRow[dc]);
                    else if (dt.Columns[colCounter].ToString() == "PRIM_LONG_DEC")
                        PRIM_LONG_DEC = Convert.ToDouble(dtRow[dc]);
                    else if ((dt.Columns[colCounter].ToString() == "STATE_NUMERIC") || (dt.Columns[colCounter].ToString() == "COUNTY_NUMERIC") || 
                        (dt.Columns[colCounter].ToString() == "ELEV_IN_M") || (dt.Columns[colCounter].ToString() == "ELEV_IN_FT"))
                    {
                        // Ensure integers are valid
                        if (int.TryParse(dtRow[dc].ToString(), out outInt))
                            doc.Add(dt.Columns[colCounter].ToString(), dtRow[dc].ToString());
                    }
                    else if ((dt.Columns[colCounter].ToString() == "DATE_CREATED") || (dt.Columns[colCounter].ToString() == "DATE_EDITED"))
                    {
                        // Apply a time offset
                        if (dtRow[dc].ToString() != "")
                        {
                            DateTimeOffset offsetTime = Convert.ToDateTime(dtRow[dc]);
                            doc.Add(dt.Columns[colCounter].ToString(), offsetTime);
                        }
                    }
                    else
                        doc.Add(dt.Columns[colCounter].ToString(), dtRow[dc].ToString());
                    colCounter++;
                }
                if ((PRIM_LAT_DEC != 0) && (PRIM_LONG_DEC != 0))
                {
                    var point = GeographyPoint.Create(Convert.ToDouble(PRIM_LAT_DEC), Convert.ToDouble(PRIM_LONG_DEC));
                    doc.Add("LOCATION", point);
                }

                indexOperations.Add(new IndexAction(IndexActionType.Upload, doc));
                rowCounter++;
                if (rowCounter > 999)
                {
                    IndexBatch(indexOperations);
                    indexOperations = new List<IndexAction>();
                    Console.WriteLine("{0} documents uploaded", rowCounter.ToString());
                    rowCounter = 0;
                }
            }
            if (rowCounter > 0)
            {
                IndexBatch(indexOperations);
                Console.WriteLine("{0} documents uploaded", rowCounter.ToString());
            }

        }

        private static System.Data.DataTable LoadCSV(string csvFileName)
        {
            // Open a CSV file and load it in to a DataTable
            try
            {
                if (!File.Exists(csvFileName))
                {
                    Console.WriteLine("File does not exist:\r\n" + csvFileName);
                    return null;
                }

                string conString = "Driver={Microsoft Text Driver (*.txt; *.csv)};Extensions=asc,csv,tab,txt;";
                System.Data.Odbc.OdbcConnection con = new System.Data.Odbc.OdbcConnection(conString);
                string commText = "SELECT * FROM [" + csvFileName + "]";
                System.Data.Odbc.OdbcDataAdapter da = new System.Data.Odbc.OdbcDataAdapter(commText, con);
                System.Data.DataTable dt = new System.Data.DataTable();
                da.Fill(dt);
                con.Close();
                con.Dispose();
                return dt;
            }
            catch (Exception ex)
            {
                Console.WriteLine("There was an error loading the CSV file:\r\n" + ex.Message);
                return null;
            }
        }


        private static void IndexBatch(List<IndexAction> changes)
        {
            // Receive a batch of documents and upload to Azure Search
            try
            {
                _indexClient.Documents.Index(new IndexBatch(changes));
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error uploading batch: {0}\r\n", ex.Message.ToString());
            }
        }

        private static void SearchDocuments(string q, string filter)
        {
            // Execute search based on query string (q) and filter 
            try
            {
                SearchParameters sp = new SearchParameters();
                if (filter != string.Empty)
                    sp.Filter = filter;
                DocumentSearchResponse response = _indexClient.Documents.Search(q, sp);
                foreach (SearchResult doc in response)
                {
                    string StoreName = doc.Document["StoreName"].ToString();
                    string Address = (doc.Document["AddressLine1"].ToString() + " " + doc.Document["AddressLine2"].ToString()).Trim();
                    string City = doc.Document["City"].ToString();
                    string Country = doc.Document["Country"].ToString();
                    Console.WriteLine("Store: {0}, Address: {1}, {2}, {3}", StoreName, Address, City, Country);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error querying index: {0}\r\n", ex.Message.ToString());
            }
        }

        static string EscapeQuotes(string colVal)
        {
            return colVal.Replace("'", "");
        }


    }
}
