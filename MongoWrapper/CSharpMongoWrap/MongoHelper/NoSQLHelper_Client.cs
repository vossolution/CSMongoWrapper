using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace VOS
{
    internal class NoSQLHelperBL
    {
        NoSQLHelper logData = new NoSQLHelper("logdata");
        NoSQLHelper dataQueues = new NoSQLHelper("dataQueues");
        NoSQLHelper mapReduceData = new NoSQLHelper("mapReduceDatas");


        Dictionary<string, string> dicEmptyParams = new Dictionary<string, string>();
        private void AddColumnsToData(DataTable dtReturnInfo)
        {
            dtReturnInfo.Columns.Add("firstColumn");
            dtReturnInfo.Columns.Add("secondColumn");
        }
        internal DataTable GetQueueInfo()
        {
            DataTable dtToBeReturned = new DataTable();
            dtToBeReturned.Columns.Add("QueueName");
            dtToBeReturned.Columns.Add("IntervalType");
            dtToBeReturned.Columns.Add("StartTime");
            dtToBeReturned.Columns.Add("infravalue");
            List<SelectParams> lsParams = new List<SelectParams>();
            SelectParams sParams = new SelectParams("StatusVal", dicEmptyParams, "$eq", "'Y'");
            //SelectParams sParams = new SelectParams("StatusVal", dicEmptyParams, "$eq", "'I'");
            lsParams.Add(sParams);
            List<List<ReturnResult>> lReturnResults = dataQueues.SelectAll(lsParams);
            foreach(List<ReturnResult> lSingleResults in lReturnResults)
            {
                DataRow drSingleRow = dtToBeReturned.NewRow();
                foreach (ReturnResult singleResult in lSingleResults)
                {
                    if(dtToBeReturned.Columns.Contains(singleResult.ColumnName))
                        drSingleRow[singleResult.ColumnName] = singleResult.ColumnValue;
                }

                dtToBeReturned.Rows.Add(drSingleRow);
            }
            return dtToBeReturned;

        }
        internal void UpdateQueueInfoToCompleted(string firstColumn, string startTime, string durationInSecs)
        {

            Dictionary<string, string> dUpdateParams = new Dictionary<string, string>();
            List<SelectParams> lSelectParams = new List<SelectParams>();

            dUpdateParams.Add("StatusVal", "'C'");

            SelectParams sParam1 = new SelectParams("firstColumn", new Dictionary<string, string>(), "$eq", "'" + firstColumn + "'");
            lSelectParams.Add(sParam1);
            SelectParams sParam2 = new SelectParams("Interval", new Dictionary<string, string>(), "$eq", "'" + durationInSecs + "'");
            lSelectParams.Add(sParam2);
            SelectParams sParam3 = new SelectParams("StartTime", new Dictionary<string, string>(), "$eq", "'" + startTime + "'");
            lSelectParams.Add(sParam3);

            dataQueues.UpdateOne(dUpdateParams, lSelectParams);
        }
        internal DataTable GetIntervalDetails(string serverName,string startTime, string endTime)
        {
            DataTable dtReturnInfo = new DataTable();
            AddColumnsToData(dtReturnInfo);
            List<SelectParams> lsParams = new List<SelectParams>();
            SelectParams sParams = new SelectParams("ServerName", dicEmptyParams, "$eq", "'" + serverName + "'");
            lsParams.Add(sParams);
            Dictionary<string, string> dicDateCompare = new Dictionary<string, string>();
            dicDateCompare.Add("$gte", startTime);
            dicDateCompare.Add("$lte", endTime);
            SelectParams sParams1 = new SelectParams("DateFloat", dicDateCompare, "", "");
            lsParams.Add(sParams1);
            List<List<ReturnResult>> lsReturnResults = logData.SelectAll(lsParams);
            foreach(List<ReturnResult> lsReturnResult in lsReturnResults)
            {
                DataRow drSingleRow = dtReturnInfo.NewRow();
                foreach(ReturnResult returnResult in lsReturnResult)
                {
                    if(dtReturnInfo.Columns.Contains(returnResult.ColumnName))
                    {
                        drSingleRow[returnResult.ColumnName] = returnResult.ColumnValue;
                    }
                }
                dtReturnInfo.Rows.Add(drSingleRow);
            }
            return dtReturnInfo;
        }
        internal void InsertData(DataTable mapReduce, string serverName)
        {
            List<Dictionary<string, string>> lsDicUnmappedUrls = new List<Dictionary<string, string>>();
            foreach(DataRow drSingleRow in mapReduce.Rows)
            {
                Dictionary<string, string> dicUnMappedUrls = new Dictionary<string, string>();
                dicUnMappedUrls.Add("DateTimeVal", "'" + drSingleRow["DateTimeVal"].ToString() + "'");
                dicUnMappedUrls.Add("MapDate", "'" + drSingleRow["MapDate"].ToString() + "'");
                dicUnMappedUrls.Add("Duration", "'" + drSingleRow["Duration"].ToString() + "'");
                dicUnMappedUrls.Add("ReduceDate", "'" + drSingleRow["ReduceDate"].ToString() + "'");
                lsDicUnmappedUrls.Add(dicUnMappedUrls);
            }
            mapReduceData.InsertMany(lsDicUnmappedUrls);
        }
    }
}

