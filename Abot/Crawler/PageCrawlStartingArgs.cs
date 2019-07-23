using Abot.Poco;
using System;

namespace Abot.Crawler
{
    [Serializable]
    public class PageCrawlStartingArgs : CrawlArgs
    {
        public PageToCrawl PageToCrawl { get; private set; }

        public PageCrawlStartingArgs(CrawlContext crawlContext, PageToCrawl pageToCrawl)
            : base(crawlContext)
        {
            PageToCrawl = pageToCrawl ?? throw new ArgumentNullException("pageToCrawl");
        }
    }
}
