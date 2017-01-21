using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Utility
{
    public static class RegExManupulation
    {
        public static string EscapeChars(string sValue, string sFormat)
        {
            string sReturn = string.Empty;
            sReturn = sValue;
            string[] sReplaceArray = sFormat.Split(new string[1] { "!@!" }, StringSplitOptions.None);

            foreach (string replacechar in sReplaceArray)
            {
                sReturn = Regex.Replace(sReturn, replacechar, string.Empty);
            }
            return sReturn;
        }
        public static string PatternReplace(string sValue, string sToken, string tokenFrom, string tokenTo)
        {
            string sUpdate = Regex.Replace(sValue, sToken, delegate(Match match)
            {
                string v = match.ToString();
                string[] sFromTokens = tokenFrom.Split(new string[1] { "!@!" }, StringSplitOptions.None);
                string[] sToTokens = tokenTo.Split(new string[1] { "!@!" }, StringSplitOptions.None);
                for (int iIndex = 0; iIndex < sFromTokens.Length; iIndex++)
                {
                    v = v.Replace(sFromTokens[iIndex], sToTokens[iIndex]);
                }
                v = v.Replace(tokenFrom, tokenTo);
                return v;
            });
            return sUpdate;
        }
        public static string ReplaceChars(string sValue, string sFormat, string sReplace)
        {
            string sReturn = string.Empty;
            sReturn = sValue;
            string[] sValueArray = sFormat.Split(new string[1] { "!@!" }, StringSplitOptions.None);
            string[] sReplaceArray = sReplace.Split(new string[1] { "!@!" }, StringSplitOptions.None);
            for (Int32 iIndex = 0; iIndex < sValueArray.Length; iIndex++)
            {
                sReturn = Regex.Replace(sReturn, sValueArray[iIndex], sReplaceArray[iIndex], RegexOptions.Compiled);
            }
            return sReturn;
        }
    }
}
