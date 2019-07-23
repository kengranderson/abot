using Abot.Poco;
using System;

namespace Abot.Crawler
{
    [Serializable]
    public class CrawlArgs : EventArgs
    {
        public CrawlContext CrawlContext { get; set; }

        public CrawlArgs(CrawlContext crawlContext)
        {
            CrawlContext = crawlContext ?? throw new ArgumentNullException("crawlContext");
        }
    }
}
