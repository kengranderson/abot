using Abot.Crawler;
using Abot.Poco;
using NUnit.Framework;
using System;

namespace Abot.Tests.Unit.Crawler
{
    [TestFixture]
    public class CrawlArgsTest
    {
        [Test]
        public void Constructor_ValidArg_SetsPublicProperty()
        {
#pragma warning disable IDE0059 // Value assigned to symbol is never used
            CrawledPage page = new CrawledPage(new Uri("http://aaa.com/"));
#pragma warning restore IDE0059 // Value assigned to symbol is never used
            CrawlContext context = new CrawlContext();
            CrawlArgs args = new CrawlArgs(context);

            Assert.AreSame(context, args.CrawlContext);
        }

        [Test]
        public void Constructor_CrawledPage_IsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new PageCrawlCompletedArgs(new CrawlContext(), null));
        }

        [Test]
        public void Constructor_CrawlContext_IsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new PageCrawlCompletedArgs(null, new CrawledPage(new Uri("http://aaa.com/"))));
        }
    }
}
