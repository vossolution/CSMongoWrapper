using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Utility
{
    public static class TimeMachine
    {
        
        public static List<string> TimeZoneList
        {
            get;
            set;
        }
        private static TimeZoneInfo ClientTimeZoneInfo = null;
        private static TimeZoneInfo SourceTimeZoneInfo = null;
        public static void SetClientTimeZone(string clientTimeZone)
        {
            try
            {
                ClientTimeZoneInfo = TimeZoneInfo.FindSystemTimeZoneById(clientTimeZone);
            }
            catch
            {
                throw;
            }
        }
        public static void SetSourceTimeZone(string destinationTimeZone)
        {
            try
            {
                SourceTimeZoneInfo = TimeZoneInfo.FindSystemTimeZoneById(destinationTimeZone);
            }
            catch
            {
                throw;
            }
        }
        public static void Initialize()
        {
            try
            {
                TimeZoneList = new List<string>();
                var timeInfos = TimeZoneInfo.GetSystemTimeZones();
                foreach (var timeInfo in timeInfos)
                {
                    TimeZoneList.Add(timeInfo.Id);
                }
            }
            catch
            {
                throw;
            }
        }
        public static DateTime ConvertTime(DateTime timeToConvert)
        {
            if (SourceTimeZoneInfo.Id != ClientTimeZoneInfo.Id)
                return TimeZoneInfo.ConvertTime(timeToConvert, SourceTimeZoneInfo, ClientTimeZoneInfo);
            else
                return timeToConvert;
        }
    }
}