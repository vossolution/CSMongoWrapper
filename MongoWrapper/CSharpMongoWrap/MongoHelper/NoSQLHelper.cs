using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using MongoDB.Bson;
using System.Configuration;

namespace VOS
{
    public class SelectParams
    {
        public string ColumnName
        {
            get; private set;
        }
        public Dictionary<string,string> ColumnFilterKeyValues
        {
            get; private set;
        }
        public string SingleFilterKey
        {
            get; private set;
        }
        public string SingleFilterValue
        {
            get; private set;
        }
        public SelectParams(string columnName, Dictionary<string,string> colFilters, string singleFilterKey, string singleFilterValue)
        {
            ColumnName = columnName;
            ColumnFilterKeyValues = colFilters;
            SingleFilterKey = singleFilterKey;
            SingleFilterValue = singleFilterValue;
        }

    }
    public class ReturnResult
    {
        public string ColumnName
        {
            get;set;
        }
        public string ColumnValue
        {
            get;set;
        }
    }
    public class NoSQLHelper
    {
        MongoClient client = null;
        IMongoDatabase database = null;
        IMongoCollection<BsonDocument> collection = null;

        public NoSQLHelper(string tableName)
        {
            string connectionString = ConfigurationManager.ConnectionStrings["NoSQLConnection"].ConnectionString;            
            client = new MongoClient(connectionString);
            var database = client.GetDatabase(ConfigurationManager.ConnectionStrings["Database"].ConnectionString);
            collection = database.GetCollection<BsonDocument>(tableName);
        }
        private BsonDocument BuildObject(Dictionary<string,string> dParam)
        {
            string retValue = "{";
            Int32 iIndex = 0;
            foreach(KeyValuePair<string,string> singleParam in dParam)
            {
                if (iIndex == 0)
                    retValue = retValue + "'" + singleParam.Key + "' : " + singleParam.Value + "";
                else
                    retValue = retValue + ",'" + singleParam.Key + "' : " + singleParam.Value + "";
                iIndex++;
            }
            retValue = retValue + "}";
            MongoDB.Bson.BsonDocument document
                = MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(retValue);
            return document;
        }

        private BsonDocument BuildUpdateObject(Dictionary<string, string> dParam)
        {
            string retValue = "{ $set: {";
            Int32 iIndex = 0;
            foreach (KeyValuePair<string, string> singleParam in dParam)
            {
                if (iIndex == 0)
                    retValue = retValue + "'" + singleParam.Key + "' : " + singleParam.Value + "";
                else
                    retValue = retValue + ",'" + singleParam.Key + "' : " + singleParam.Value + "";
                iIndex++;
            }
            retValue = retValue + "} }";
            MongoDB.Bson.BsonDocument document
                = MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(retValue);
            return document;
        }

        private UpdateDefinition<BsonDocument> BuildUpdateParams(List<SelectParams> selectParameters)
        {

            UpdateDefinition<BsonDocument> toReturn = null;
            string sReturnValue = string.Empty;
            foreach (SelectParams singleParam in selectParameters)
            {
                //singleParam.ColumnName;
                //singleParam.ColumnFilterKeyValues;
                //singleParam.SingleFilterKey;
                //singleParam.SingleFilterValue;
                if (singleParam.ColumnFilterKeyValues.Count > 0)
                {
                    //"{ testId: {$gte: 3345, $lte: 3400} }"
                    if (sReturnValue == string.Empty)
                        sReturnValue = sReturnValue + "{" + singleParam.ColumnName + ": {";
                    else
                        sReturnValue = sReturnValue + ",{" + singleParam.ColumnName + ": {";

                    Int32 iIndex = 0;
                    foreach (KeyValuePair<string, string> singleKeyVal in singleParam.ColumnFilterKeyValues)
                    {
                        if (iIndex == 0)
                        {
                            iIndex = iIndex + 1;
                            sReturnValue = sReturnValue + singleKeyVal.Key + ": " + singleKeyVal.Value;
                        }
                        else
                        {
                            sReturnValue = sReturnValue + ", " + singleKeyVal.Key + ": " + singleKeyVal.Value;
                        }
                    }

                    sReturnValue = sReturnValue + "} }";
                }
                else
                {
                    if (sReturnValue == string.Empty)
                        sReturnValue = sReturnValue + "{" + singleParam.ColumnName + ": {" + singleParam.SingleFilterKey + ": " + singleParam.SingleFilterValue + "} }";
                    else
                        sReturnValue = sReturnValue + ",{" + singleParam.ColumnName + ": {" + singleParam.SingleFilterKey + ": " + singleParam.SingleFilterValue + "} }";
                }
            }
            toReturn = sReturnValue;
            return toReturn;
        }

        private FilterDefinition<BsonDocument> BuildSelectParams(List<SelectParams> selectParameters)
        {

            FilterDefinition<BsonDocument> toReturn = null;
            string sReturnValue = string.Empty;
            foreach(SelectParams singleParam in selectParameters)
            {
                //singleParam.ColumnName;
                //singleParam.ColumnFilterKeyValues;
                //singleParam.SingleFilterKey;
                //singleParam.SingleFilterValue;
                if(singleParam.ColumnFilterKeyValues.Count > 0)
                {
                    //"{ testId: {$gte: 3345, $lte: 3400} }"
                    if (sReturnValue == string.Empty)
                        sReturnValue = sReturnValue + singleParam.ColumnName + ": {";
                    else
                        sReturnValue = sReturnValue + "," + singleParam.ColumnName + ": {";

                    Int32 iIndex = 0;
                    foreach(KeyValuePair<string,string> singleKeyVal in singleParam.ColumnFilterKeyValues)
                    {
                        if (iIndex == 0)
                        {
                            iIndex = iIndex + 1;
                            sReturnValue = sReturnValue + singleKeyVal.Key + ": " + singleKeyVal.Value;
                        }
                        else
                        {
                            sReturnValue = sReturnValue + ", " + singleKeyVal.Key + ": " + singleKeyVal.Value;
                        }
                    }

                    sReturnValue = sReturnValue + "}";
                }
                else
                {
                    if (sReturnValue == string.Empty)
                    {
                        sReturnValue = sReturnValue + singleParam.ColumnName + ": {" + singleParam.SingleFilterKey + ": " + singleParam.SingleFilterValue + "}";
                    }
                    else
                    {
                        sReturnValue = sReturnValue + "," + singleParam.ColumnName + ": {" + singleParam.SingleFilterKey + ": " + singleParam.SingleFilterValue + "}";
                    }
                }
            }
            toReturn = "{" + sReturnValue + "}";
            return toReturn;
        }

        public void InsertOne(Dictionary<string,string> dParams)
        {
            BsonDocument bDocument = BuildObject(dParams);
            collection.InsertOne(bDocument);
        }
        public void InsertMany(List<Dictionary<string, string>> dParams)
        {
            List<BsonDocument> bDcouments = new List<BsonDocument>();
            Int32 iIndex = 0;
            foreach(Dictionary<string,string> dParam in dParams)
            {
                iIndex++;
                try
                {
                    bDcouments.Add(BuildObject(dParam));
                }
                catch (Exception ex)
                {

                }
            }
            collection.InsertMany(bDcouments);
        }
        public List<List<ReturnResult>> GetDetails(List<BsonDocument> returnDetails)
        {
            List<List<ReturnResult>> multiResults = new List<List<VOS.ReturnResult>>();
            foreach (var singleResult in returnDetails)
            {
                List<ReturnResult> returnResults = new List<VOS.ReturnResult>();
                var subRestuls = singleResult.Elements.ToList();
                foreach (var singleSub in subRestuls)
                {
                    ReturnResult singleResultOut = new ReturnResult();
                    singleResultOut.ColumnName = singleSub.Name;
                    singleResultOut.ColumnValue = singleSub.Value.RawValue.ToString();
                    returnResults.Add(singleResultOut);
                }
                multiResults.Add(returnResults);
            }
            return multiResults;
        }
        public List<List<ReturnResult>> SelectAll(List<SelectParams> selectParameters)
        {
            FilterDefinition<BsonDocument> fParams = BuildSelectParams(selectParameters);
            var returnDetails = collection.Find(fParams).ToList();
            List<List<ReturnResult>> returnResults = GetDetails(returnDetails);            
            return returnResults;
        }
        public List<List<ReturnResult>> SelectAllDistinct(List<SelectParams> selectParameters)
        {
            FilterDefinition<BsonDocument> fParams = BuildSelectParams(selectParameters);
            var returnDetails = collection.Find(fParams).ToList().Distinct().ToList();
            List<List<ReturnResult>> returnResults = GetDetails(returnDetails);
            return returnResults;
        }
        public void DeleteOne(List<SelectParams> selectParameters)
        {
            FilterDefinition<BsonDocument> fParams = BuildSelectParams(selectParameters);
            collection.DeleteOne(fParams);
        }
        public void DeleteMany(List<SelectParams> selectParameters)
        {
            FilterDefinition<BsonDocument> fParams = BuildSelectParams(selectParameters);
            collection.DeleteMany(fParams);
        }
        public void UpdateOne(Dictionary<string,string> dSetParams, List<SelectParams> dWhereParams)
        {
            FilterDefinition<BsonDocument> fWhereParams = BuildSelectParams(dWhereParams);
            UpdateDefinition<BsonDocument> fSetParams = BuildUpdateObject(dSetParams);
            collection.UpdateOne(fWhereParams, fSetParams);
        }
    }
}
