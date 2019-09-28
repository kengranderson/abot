using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Abot2.Core;
using Abot2.Poco;
using Abot2.Util;
using Timer = System.Timers.Timer;
using NLog;
using System.Linq;

namespace Abot2.Crawler
{
    public interface IWebCrawler : IDisposable
    {
        /// <summary>
        /// Event that is fired before a page is crawled.
        /// </summary>
        event EventHandler<PageCrawlStartingArgs> PageCrawlStarting;

        /// <summary>
        /// Event that is fired when an individual page has been crawled.
        /// </summary>
        event EventHandler<PageCrawlCompletedArgs> PageCrawlCompleted;

        /// <summary>
        /// Event that is fired when the ICrawlDecisionMaker.ShouldCrawl impl returned false. This means the page or its links were not crawled.
        /// </summary>
        event EventHandler<PageCrawlDisallowedArgs> PageCrawlDisallowed;

        /// <summary>
        /// Event that is fired when the ICrawlDecisionMaker.ShouldCrawlLinks impl returned false. This means the page's links were not crawled.
        /// </summary>
        event EventHandler<PageLinksCrawlDisallowedArgs> PageLinksCrawlDisallowed;


        /// <summary>
        /// Delegate to be called to determine whether a page should be crawled or not
        /// </summary>
        Func<PageToCrawl, CrawlContext, CrawlDecision> ShouldCrawlPageDecisionMaker { get; set; }
    
        /// <summary>
        /// Delegate to be called to determine whether the page's content should be downloaded
        /// </summary>
        Func<CrawledPage, CrawlContext, CrawlDecision> ShouldDownloadPageContentDecisionMaker { get; set;}

        /// <summary>
        /// Delegate to be called to determine whether a page's links should be crawled or not
        /// </summary>
        Func<CrawledPage, CrawlContext, CrawlDecision> ShouldCrawlPageLinksDecisionMaker { get; set; }

        /// <summary>
        /// Delegate to be called to determine whether a certain link on a page should be scheduled to be crawled
        /// </summary>
        Func<Uri, CrawledPage, CrawlContext, bool> ShouldScheduleLinkDecisionMaker { get; set; }

        /// <summary>
        /// Delegate to be called to determine whether a page should be recrawled
        /// </summary>
        Func<CrawledPage, CrawlContext, CrawlDecision> ShouldRecrawlPageDecisionMaker { get; set; }

        /// <summary>
        /// Delegate to be called to determine whether the 1st uri param is considered an internal uri to the second uri param
        /// </summary>
        Func<Uri, Uri, bool> IsInternalUriDecisionMaker { get; set; }


        /// <summary>
        /// Begins a crawl using the uri param
        /// </summary>
        Task<CrawlResult> CrawlAsync(Uri uri);

        /// <summary>
        /// Begins a crawl using the uri param, and can be cancelled using the CancellationToken
        /// </summary>
        Task<CrawlResult> CrawlAsync(Uri uri, CancellationTokenSource tokenSource);

        /// <summary>
        /// Dynamic object that can hold any value that needs to be available in the crawl context
        /// </summary>
        dynamic CrawlBag { get; set; }
    }

    public abstract class WebCrawler : IWebCrawler
    {
        protected static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        protected bool _crawlComplete = false;
        protected bool _crawlStopReported = false;
        protected bool _crawlCancellationReported = false;
        protected bool _maxPagesToCrawlLimitReachedOrScheduled = false;
        protected Timer _timeoutTimer;
        protected CrawlResult _crawlResult = null;
        protected CrawlContext _crawlContext;
        protected IThreadManager _threadManager;
        protected IScheduler _scheduler;
        protected IPageRequester _pageRequester;
        protected IHtmlParser _htmlParser;
        protected ICrawlDecisionMaker _crawlDecisionMaker;
        protected IMemoryManager _memoryManager;
        object _locker = new object();
        int _pagesToCrawl = 0;

        #region Public properties

        /// <summary>
        /// Dynamic object that can hold any value that needs to be available in the crawl context
        /// </summary>
        public dynamic CrawlBag { get; set; }

        /// <inheritdoc />
        public Func<PageToCrawl, CrawlContext, CrawlDecision> ShouldCrawlPageDecisionMaker { get; set; }

        /// <inheritdoc />
        public Func<CrawledPage, CrawlContext, CrawlDecision> ShouldDownloadPageContentDecisionMaker { get; set; }

        /// <inheritdoc />
        public Func<CrawledPage, CrawlContext, CrawlDecision> ShouldCrawlPageLinksDecisionMaker { get; set; }

        /// <inheritdoc />
        public Func<CrawledPage, CrawlContext, CrawlDecision> ShouldRecrawlPageDecisionMaker { get; set; }

        /// <inheritdoc />
        public Func<Uri, CrawledPage, CrawlContext, bool> ShouldScheduleLinkDecisionMaker { get; set; }

        /// <inheritdoc />
        public Func<Uri, Uri, bool> IsInternalUriDecisionMaker { get; set; } = (uriInQuestion, rootUri) => uriInQuestion.Authority == rootUri.Authority;

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a crawler instance with the default settings and implementations.
        /// </summary>
        public WebCrawler()
            : this(new CrawlConfiguration(), null, null, null, null, null, null)
        {
        }

        /// <summary>
        /// Creates a crawler instance with custom settings or implementation. Passing in null for all params is the equivalent of the empty constructor.
        /// </summary>
        /// <param name="threadManager">Distributes http requests over multiple threads</param>
        /// <param name="scheduler">Decides what link should be crawled next</param>
        /// <param name="pageRequester">Makes the raw http requests</param>
        /// <param name="htmlParser">Parses a crawled page for it's hyperlinks</param>
        /// <param name="crawlDecisionMaker">Decides whether or not to crawl a page or that page's links</param>
        /// <param name="crawlConfiguration">Configurable crawl values</param>
        /// <param name="memoryManager">Checks the memory usage of the host process</param>
        public WebCrawler(
            CrawlConfiguration crawlConfiguration,
            ICrawlDecisionMaker crawlDecisionMaker,
            IThreadManager threadManager,
            IScheduler scheduler,
            IPageRequester pageRequester,
            IHtmlParser htmlParser,
            IMemoryManager memoryManager)
        {
            _crawlContext = new CrawlContext
            {
                CrawlConfiguration = crawlConfiguration ?? new CrawlConfiguration()
            };
            CrawlBag = _crawlContext.CrawlBag;

            _threadManager = threadManager ?? new TaskThreadManager(_crawlContext.CrawlConfiguration.MaxConcurrentThreads > 0 ? _crawlContext.CrawlConfiguration.MaxConcurrentThreads : Environment.ProcessorCount);
            _scheduler = scheduler ?? new Scheduler(_crawlContext.CrawlConfiguration.IsUriRecrawlingEnabled, null, null);
            _pageRequester = pageRequester ?? new PageRequester(_crawlContext.CrawlConfiguration, new WebContentExtractor());
            _crawlDecisionMaker = crawlDecisionMaker ?? new CrawlDecisionMaker();

            if (_crawlContext.CrawlConfiguration.MaxMemoryUsageInMb > 0
                || _crawlContext.CrawlConfiguration.MinAvailableMemoryRequiredInMb > 0)
                _memoryManager = memoryManager ?? new MemoryManager(new CachedMemoryMonitor(new GcMemoryMonitor(), _crawlContext.CrawlConfiguration.MaxMemoryUsageCacheTimeInSeconds));

            _htmlParser = htmlParser ?? new AngleSharpHyperlinkParser(_crawlContext.CrawlConfiguration, null);

            _crawlContext.Scheduler = _scheduler;
        }

        #endregion Constructors

        #region Public Methods

        /// <inheritdoc />
        public virtual Task<CrawlResult> CrawlAsync(Uri uri) => CrawlAsync(uri, null);

        /// <inheritdoc />
        public virtual async Task<CrawlResult> CrawlAsync(Uri uri, CancellationTokenSource cancellationTokenSource)
        {
            if (uri == null)
                throw new ArgumentNullException(nameof(uri));

            _crawlContext.RootUri = _crawlContext.OriginalRootUri = uri;

            if (cancellationTokenSource != null)
                _crawlContext.CancellationTokenSource = cancellationTokenSource;

            _crawlResult = new CrawlResult
            {
                RootUri = _crawlContext.RootUri,
                CrawlContext = _crawlContext
            };
            _crawlComplete = false;

            _logger.Info($"About to crawl site [{uri.AbsoluteUri}]");
            PrintConfigValues(_crawlContext.CrawlConfiguration);

            if (_memoryManager != null)
            {
                _crawlContext.MemoryUsageBeforeCrawlInMb = _memoryManager.GetCurrentUsageInMb();
                _logger.Info($"Starting memory usage for site [{uri.AbsoluteUri}] is [{_crawlContext.MemoryUsageBeforeCrawlInMb}mb]");
            }

            _crawlContext.CrawlStartDate = DateTime.Now;
            var timer = Stopwatch.StartNew();

            if (_crawlContext.CrawlConfiguration.CrawlTimeoutSeconds > 0)
            {
                _timeoutTimer = new Timer(_crawlContext.CrawlConfiguration.CrawlTimeoutSeconds * 1000);
                _timeoutTimer.Elapsed += HandleCrawlTimeout;
                _timeoutTimer.Start();
            }

            try
            {
                var rootPage = new PageToCrawl(uri) { ParentUri = uri, IsInternal = true, IsRoot = true };
                if (ShouldSchedulePageLink(rootPage))
                    _scheduler.Add(rootPage);

                VerifyRequiredAvailableMemory();
                await CrawlSite().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _crawlResult.ErrorException = e;
                _logger.Fatal($"An error occurred while crawling site [{uri}]");
                _logger.Fatal(e, "Exception details -->");
            }
            finally
            {
                if (_threadManager != null)
                    _threadManager.Dispose();
            }

            if (_timeoutTimer != null)
                _timeoutTimer.Stop();

            timer.Stop();

            if (_memoryManager != null)
            {
                _crawlContext.MemoryUsageAfterCrawlInMb = _memoryManager.GetCurrentUsageInMb();
                _logger.Info($"Ending memory usage for site [{uri.AbsoluteUri}] is [{_crawlContext.MemoryUsageAfterCrawlInMb}mb]");
            }

            _crawlResult.Elapsed = timer.Elapsed;
            _logger.Info($"Crawl complete for site [{_crawlResult.RootUri.AbsoluteUri}]: Crawled [{_crawlResult.CrawlContext.CrawledCount}] pages in [{_crawlResult.Elapsed}]");

            return _crawlResult;
        }

        /// <inheritdoc />
        public virtual void Dispose()
        {
            if (_threadManager != null)
            {
                _threadManager.Dispose();
            }
            if (_scheduler != null)
            {
                _scheduler.Dispose();
            }
            if (_pageRequester != null)
            {
                _pageRequester.Dispose();
            }
            if (_memoryManager != null)
            {
                _memoryManager.Dispose();
            }
        }

        #endregion

        #region Events

        /// <inheritdoc />
        public event EventHandler<PageCrawlStartingArgs> PageCrawlStarting;

        /// <inheritdoc />
        public event EventHandler<PageCrawlCompletedArgs> PageCrawlCompleted;

        /// <inheritdoc />
        public event EventHandler<PageCrawlDisallowedArgs> PageCrawlDisallowed;

        /// <inheritdoc />
        public event EventHandler<PageLinksCrawlDisallowedArgs> PageLinksCrawlDisallowed;

        protected virtual void FirePageCrawlStartingEvent(PageToCrawl pageToCrawl)
        {
            try
            {
                PageCrawlStarting?.Invoke(this, new PageCrawlStartingArgs(_crawlContext, pageToCrawl));
            }
            catch (Exception e)
            {
                _logger.Error("An unhandled exception was thrown by a subscriber of the PageCrawlStarting event for url:" + pageToCrawl.Uri.AbsoluteUri);
                _logger.Error(e, "Exception details -->");
            }
        }

        protected virtual void FirePageCrawlCompletedEvent(CrawledPage crawledPage)
        {
            try
            {
                PageCrawlCompleted?.Invoke(this, new PageCrawlCompletedArgs(_crawlContext, crawledPage));
            }
            catch (Exception e)
            {
                _logger.Error("An unhandled exception was thrown by a subscriber of the PageCrawlCompleted event for url:" + crawledPage.Uri.AbsoluteUri);
                _logger.Error(e, "Exception details -->");
            }
        }

        protected virtual void FirePageCrawlDisallowedEvent(PageToCrawl pageToCrawl, string reason)
        {
            try
            {
                PageCrawlDisallowed?.Invoke(this, new PageCrawlDisallowedArgs(_crawlContext, pageToCrawl, reason));
            }
            catch (Exception e)
            {
                _logger.Error("An unhandled exception was thrown by a subscriber of the PageCrawlDisallowed event for url:" + pageToCrawl.Uri.AbsoluteUri);
                _logger.Error(e, "Exception details -->");
            }
        }

        protected virtual void FirePageLinksCrawlDisallowedEvent(CrawledPage crawledPage, string reason)
        {
            try
            {
                PageLinksCrawlDisallowed?.Invoke(this, new PageLinksCrawlDisallowedArgs(_crawlContext, crawledPage, reason));
            }
            catch (Exception e)
            {
                _logger.Error("An unhandled exception was thrown by a subscriber of the PageLinksCrawlDisallowed event for url:" + crawledPage.Uri.AbsoluteUri);
                _logger.Error(e, "Exception details -->");
            }
        }

        #endregion

        #region Protected Async Methods

        protected virtual async Task CrawlSite()
        {
            _logger.Debug("Entering CrawlSite");

            while (!_crawlComplete)
            {
                RunPreWorkChecks();

                if (_scheduler.Count > 0)
                {
                    _logger.Debug($"Scheduler.Count: {_scheduler.Count}");
                    _threadManager.DoWork(() => ProcessPage(_scheduler.GetNext()));
                }
                else if (!_threadManager.HasRunningThreads() && _pagesToCrawl < 1)
                {
                    _logger.Debug("No more running threads - crawl complete");
                    _crawlComplete = true;
                }
                else
                {
                    _logger.Debug("Waiting for links to be scheduled...");
                    await Task.Delay(2500).ConfigureAwait(false);
                }
            }
        }

        protected virtual async void ProcessPage(PageToCrawl pageToCrawl)
        {
            lock (_locker)
            {
                ++_pagesToCrawl;
                _logger.Debug($"ProcessPage: {_pagesToCrawl} pending pages to crawl.");
            }

            try
            {
                if (pageToCrawl == null)
                    return;

                ThrowIfCancellationRequested();

                AddPageToContext(pageToCrawl);

                var crawledPage = await CrawlThePage(pageToCrawl).ConfigureAwait(false);

                // Validate the root uri in case of a redirection.
                if (crawledPage.IsRoot)
                    ValidateRootUriForRedirection(crawledPage);

                if (IsRedirect(crawledPage) && !_crawlContext.CrawlConfiguration.IsHttpRequestAutoRedirectsEnabled)
                    ProcessRedirect(crawledPage);

                if (PageSizeIsAboveMax(crawledPage))
                    return;

                ThrowIfCancellationRequested();

                var shouldCrawlPageLinks = ShouldCrawlPageLinks(crawledPage);
                if (shouldCrawlPageLinks || _crawlContext.CrawlConfiguration.IsForcedLinkParsingEnabled)
                    ParsePageLinks(crawledPage);

                ThrowIfCancellationRequested();

                if (shouldCrawlPageLinks)
                    SchedulePageLinks(crawledPage);

                ThrowIfCancellationRequested();

                FirePageCrawlCompletedEvent(crawledPage);

                if (ShouldRecrawlPage(crawledPage))
                {
                    crawledPage.IsRetry = true;
                    _scheduler.Add(crawledPage);
                }
            }
            catch (OperationCanceledException)
            {
                _logger.Debug($"Thread cancelled while crawling/processing page [{pageToCrawl.Uri}]");
                throw;
            }
            catch (Exception e)
            {
                _crawlResult.ErrorException = e;
                _logger.Fatal($"Error occurred during processing of page [{pageToCrawl.Uri}]");
                _logger.Fatal(e, "Exception details -->");

                _crawlContext.IsCrawlHardStopRequested = true;
            }
            finally
            {
                lock (_locker)
                {
                    --_pagesToCrawl;
                    _logger.Debug($"ProcessPage: {_pagesToCrawl} pending pages to crawl.");
                }
            }
        }

        protected virtual async Task<CrawledPage> CrawlThePage(PageToCrawl pageToCrawl)
        {
            _logger.Debug($"About to crawl page [{pageToCrawl.Uri.AbsoluteUri}]");
            FirePageCrawlStartingEvent(pageToCrawl);

            if (pageToCrawl.IsRetry) { WaitMinimumRetryDelay(pageToCrawl); }

            pageToCrawl.LastRequest = DateTime.Now;

            var crawledPage = await _pageRequester.MakeRequestAsync(pageToCrawl.Uri, ShouldDownloadPageContent).ConfigureAwait(false);

            Map(pageToCrawl, crawledPage);

            if (crawledPage.HttpResponseMessage == null)
                _logger.Info($"Page crawl complete, Status:[NA] Url:[{crawledPage.Uri.AbsoluteUri}] Elapsed:[{crawledPage.Elapsed}] Parent:[{crawledPage.ParentUri}] Retry:[{crawledPage.RetryCount}]");
            else
                _logger.Info($"Page crawl complete, Status:[{Convert.ToInt32(crawledPage.HttpResponseMessage.StatusCode)}] Url:[{crawledPage.Uri.AbsoluteUri}] Elapsed:[{crawledPage.Elapsed}] Parent:[{crawledPage.ParentUri}] Retry:[{crawledPage.RetryCount}]");

            return crawledPage;
        }

        #endregion

        #region Procted Methods

        protected virtual void VerifyRequiredAvailableMemory()
        {
            if (_crawlContext.CrawlConfiguration.MinAvailableMemoryRequiredInMb < 1)
                return;

            if (!_memoryManager.IsSpaceAvailable(_crawlContext.CrawlConfiguration.MinAvailableMemoryRequiredInMb))
                throw new InsufficientMemoryException($"Process does not have the configured [{_crawlContext.CrawlConfiguration.MinAvailableMemoryRequiredInMb}mb] of available memory to crawl site [{_crawlContext.RootUri}]. This is configurable through the minAvailableMemoryRequiredInMb in app.conf or CrawlConfiguration.MinAvailableMemoryRequiredInMb.");
        }

        protected virtual void RunPreWorkChecks()
        {
            CheckMemoryUsage();
            CheckForCancellationRequest();
            CheckForHardStopRequest();
            CheckForStopRequest();
        }

        protected virtual void CheckMemoryUsage()
        {
            if (_memoryManager == null
                || _crawlContext.IsCrawlHardStopRequested
                || _crawlContext.CrawlConfiguration.MaxMemoryUsageInMb < 1)
                return;
            
            var currentMemoryUsage = _memoryManager.GetCurrentUsageInMb();
            if (_logger.IsDebugEnabled)
                _logger.Debug($"Current memory usage for site [{_crawlContext.RootUri}] is [{currentMemoryUsage}mb]");

            if (currentMemoryUsage > _crawlContext.CrawlConfiguration.MaxMemoryUsageInMb)
            {
                _memoryManager.Dispose();
                _memoryManager = null;

                var message = $"Process is using [{currentMemoryUsage}mb] of memory which is above the max configured of [{_crawlContext.CrawlConfiguration.MaxMemoryUsageInMb}mb] for site [{_crawlContext.RootUri}]. This is configurable through the maxMemoryUsageInMb in app.conf or CrawlConfiguration.MaxMemoryUsageInMb.";
                _crawlResult.ErrorException = new InsufficientMemoryException(message);

                _logger.Fatal(_crawlResult.ErrorException, "Exception details -->");
                _crawlContext.IsCrawlHardStopRequested = true;
            }
        }

        protected virtual void CheckForCancellationRequest()
        {
            if (_crawlContext.CancellationTokenSource.IsCancellationRequested)
            {
                if (!_crawlCancellationReported)
                {
                    var message = $"Crawl cancellation requested for site [{_crawlContext.RootUri}]!";
                    _logger.Fatal(message);
                    _crawlResult.ErrorException = new OperationCanceledException(message, _crawlContext.CancellationTokenSource.Token);
                    _crawlContext.IsCrawlHardStopRequested = true;
                    _crawlCancellationReported = true;
                }
            }
        }

        protected virtual void CheckForHardStopRequest()
        {
            if (_crawlContext.IsCrawlHardStopRequested)
            {
                if (!_crawlStopReported)
                {
                    _logger.Info($"Hard crawl stop requested for site [{_crawlContext.RootUri}]!");
                    _crawlStopReported = true;
                }

                _scheduler.Clear();
                _threadManager.AbortAll();
                _scheduler.Clear();//to be sure nothing was scheduled since first call to clear()

                //Set all events to null so no more events are fired
                PageCrawlStarting = null;
                PageCrawlCompleted = null;
                PageCrawlDisallowed = null;
                PageLinksCrawlDisallowed = null;
            }
        }

        protected virtual void CheckForStopRequest()
        {
            if (_crawlContext.IsCrawlStopRequested)
            {
                if (!_crawlStopReported)
                {
                    _logger.Info($"Crawl stop requested for site [{_crawlContext.RootUri}]!");
                    _crawlStopReported = true;
                }
                _scheduler.Clear();
            }
        }

        protected virtual void HandleCrawlTimeout(object sender, ElapsedEventArgs e)
        {
            if (sender is Timer elapsedTimer)
                elapsedTimer.Stop();

            _logger.Warn($"Crawl timeout of [{_crawlContext.CrawlConfiguration.CrawlTimeoutSeconds}] seconds has been reached for [{_crawlContext.RootUri}]");
            _crawlContext.IsCrawlHardStopRequested = true;
        }

        protected virtual void ProcessRedirect(CrawledPage crawledPage)
        {
            if (crawledPage.RedirectPosition >= 20)
                _logger.Warn($"Page [{crawledPage.Uri}] is part of a chain of 20 or more consecutive redirects, redirects for this chain will now be aborted.");
                
            try
            {
                var uri = ExtractRedirectUri(crawledPage);

                var page = new PageToCrawl(uri)
                {
                    ParentUri = crawledPage.ParentUri,
                    CrawlDepth = crawledPage.CrawlDepth,
                    IsInternal = IsInternalUri(uri),
                    IsRoot = false,
                    RedirectedFrom = crawledPage,
                    RedirectPosition = crawledPage.RedirectPosition + 1
                };

                crawledPage.RedirectedTo = page;
                _logger.Debug($"Page [{crawledPage.Uri}] is requesting that it be redirect to [{crawledPage.RedirectedTo.Uri}]");

                if (ShouldSchedulePageLink(page))
                {
                    _logger.Info($"Page [{crawledPage.Uri}] will be redirect to [{crawledPage.RedirectedTo.Uri}]");
                    _scheduler.Add(page);
                }
            }
            catch {}
        }

        protected virtual bool IsInternalUri(Uri uri)
        {
            return  IsInternalUriDecisionMaker(uri, _crawlContext.RootUri) ||
                IsInternalUriDecisionMaker(uri, _crawlContext.OriginalRootUri);
        }

        protected virtual bool IsRedirect(CrawledPage crawledPage)
        {
            var isRedirect = false;
            if (crawledPage.HttpResponseMessage != null) {
                isRedirect = (_crawlContext.CrawlConfiguration.IsHttpRequestAutoRedirectsEnabled &&
                    crawledPage.HttpResponseMessage.RequestMessage.RequestUri != null &&
                    crawledPage.HttpResponseMessage.RequestMessage.RequestUri.AbsoluteUri != crawledPage.Uri.AbsoluteUri) ||
                    (!_crawlContext.CrawlConfiguration.IsHttpRequestAutoRedirectsEnabled &&
                    (int) crawledPage.HttpResponseMessage.StatusCode >= 300 &&
                    (int)crawledPage.HttpResponseMessage.StatusCode <= 399);
            }
            return isRedirect;
        }

        protected virtual void ThrowIfCancellationRequested()
        {
            if (_crawlContext.CancellationTokenSource != null && _crawlContext.CancellationTokenSource.IsCancellationRequested)
                _crawlContext.CancellationTokenSource.Token.ThrowIfCancellationRequested();
        }

        protected virtual bool PageSizeIsAboveMax(CrawledPage crawledPage)
        {
            var isAboveMax = false;
            if (_crawlContext.CrawlConfiguration.MaxPageSizeInBytes > 0 &&
                crawledPage.Content.Bytes != null && 
                crawledPage.Content.Bytes.Length > _crawlContext.CrawlConfiguration.MaxPageSizeInBytes)
            {
                isAboveMax = true;
                _logger.Warn($"Page [{crawledPage.Uri}] has a page size of [{crawledPage.Content.Bytes.Length}] bytes which is above the [{_crawlContext.CrawlConfiguration.MaxPageSizeInBytes}] byte max, no further processing will occur for this page");
            }
            return isAboveMax;
        }

        protected virtual bool ShouldCrawlPageLinks(CrawledPage crawledPage)
        {
            var shouldCrawlPageLinksDecision = _crawlDecisionMaker.ShouldCrawlPageLinks(crawledPage, _crawlContext);
            if (shouldCrawlPageLinksDecision.Allow)
                shouldCrawlPageLinksDecision = (ShouldCrawlPageLinksDecisionMaker != null) ? ShouldCrawlPageLinksDecisionMaker.Invoke(crawledPage, _crawlContext) : new CrawlDecision { Allow = true };

            if (!shouldCrawlPageLinksDecision.Allow)
            {
                _logger.Warn($"Links on page [{crawledPage.Uri.AbsoluteUri}] not crawled, [{shouldCrawlPageLinksDecision.Reason}]");
                FirePageLinksCrawlDisallowedEvent(crawledPage, shouldCrawlPageLinksDecision.Reason);
            }

            SignalCrawlStopIfNeeded(shouldCrawlPageLinksDecision);
            return shouldCrawlPageLinksDecision.Allow;
        }

        protected virtual bool ShouldCrawlPage(PageToCrawl pageToCrawl)
        {
            if (_maxPagesToCrawlLimitReachedOrScheduled)
                return false;

            var shouldCrawlPageDecision = _crawlDecisionMaker.ShouldCrawlPage(pageToCrawl, _crawlContext);
            if (!shouldCrawlPageDecision.Allow &&
                shouldCrawlPageDecision.Reason.Contains("MaxPagesToCrawl limit of"))
            {
                _maxPagesToCrawlLimitReachedOrScheduled = true;
                _logger.Warn($"MaxPagesToCrawlLimit has been reached or scheduled. No more pages will be scheduled.");
                return false;
            }

            if (shouldCrawlPageDecision.Allow)
                shouldCrawlPageDecision = (ShouldCrawlPageDecisionMaker != null) ? ShouldCrawlPageDecisionMaker(pageToCrawl, _crawlContext) : new CrawlDecision { Allow = true };

            if (!shouldCrawlPageDecision.Allow)
            {
                _logger.Warn($"Page [{pageToCrawl.Uri.AbsoluteUri}] not crawled, [{shouldCrawlPageDecision.Reason}]");
                FirePageCrawlDisallowedEvent(pageToCrawl, shouldCrawlPageDecision.Reason);
            }

            SignalCrawlStopIfNeeded(shouldCrawlPageDecision);
            return shouldCrawlPageDecision.Allow;
        }

        protected virtual bool ShouldRecrawlPage(CrawledPage crawledPage)
        {
            //TODO No unit tests cover these lines
            var shouldRecrawlPageDecision = _crawlDecisionMaker.ShouldRecrawlPage(crawledPage, _crawlContext);
            if (shouldRecrawlPageDecision.Allow)
                shouldRecrawlPageDecision = (ShouldRecrawlPageDecisionMaker != null) ? ShouldRecrawlPageDecisionMaker(crawledPage, _crawlContext) : new CrawlDecision { Allow = true };

            if (!shouldRecrawlPageDecision.Allow)
            {
                _logger.Debug($"Page [{crawledPage.Uri.AbsoluteUri}] not recrawled, [{shouldRecrawlPageDecision.Reason}]");
            }
            else
            {
                // Look for the Retry-After header in the response.
                crawledPage.RetryAfter = null;

                var value = crawledPage.HttpResponseMessage?.Headers?.RetryAfter?.ToString();
                if (!String.IsNullOrEmpty(value))
                {
                    // Try to convert to DateTime first, then in double.
                    if (crawledPage.LastRequest.HasValue && DateTime.TryParse(value, out DateTime date))
                    {
                        crawledPage.RetryAfter = (date - crawledPage.LastRequest.Value).TotalSeconds;
                    }
                    else if (double.TryParse(value, out double seconds))
                    {
                        crawledPage.RetryAfter = seconds;
                    }
                }
            }

            SignalCrawlStopIfNeeded(shouldRecrawlPageDecision);
            return shouldRecrawlPageDecision.Allow;
        }

        protected virtual void Map(PageToCrawl src, CrawledPage dest)
        {
            dest.Uri = src.Uri;
            dest.ParentUri = src.ParentUri;
            dest.IsRetry = src.IsRetry;
            dest.RetryAfter = src.RetryAfter;
            dest.RetryCount = src.RetryCount;
            dest.LastRequest = src.LastRequest;
            dest.IsRoot = src.IsRoot;
            dest.IsInternal = src.IsInternal;
            dest.PageBag = CombinePageBags(src.PageBag, dest.PageBag);
            dest.CrawlDepth = src.CrawlDepth;
            dest.RedirectedFrom = src.RedirectedFrom;
            dest.RedirectPosition = src.RedirectPosition;
        }

        protected virtual dynamic CombinePageBags(dynamic pageToCrawlBag, dynamic crawledPageBag)
        {
            IDictionary<string, object> combinedBag = new ExpandoObject();
            var pageToCrawlBagDict = pageToCrawlBag as IDictionary<string, object>;
            var crawledPageBagDict = crawledPageBag as IDictionary<string, object>;

            foreach (var entry in pageToCrawlBagDict) combinedBag[entry.Key] = entry.Value;
            foreach (var entry in crawledPageBagDict) combinedBag[entry.Key] = entry.Value;

            return combinedBag;
        }

        protected virtual void AddPageToContext(PageToCrawl pageToCrawl)
        {
            if (pageToCrawl.IsRetry)
            {
                pageToCrawl.RetryCount++;
                return;
            }

            Interlocked.Increment(ref _crawlContext.CrawledCount);
            _crawlContext.CrawlCountByDomain.AddOrUpdate(pageToCrawl.Uri.Authority, 1, (key, oldValue) => oldValue + 1);
        }

        protected virtual void ParsePageLinks(CrawledPage crawledPage)
        {
            crawledPage.ParsedLinks = _htmlParser.GetLinks(crawledPage);
            _logger.Info($"Parsed {crawledPage.ParsedLinks.Count()} links in url {crawledPage.Uri}");
        }

        protected virtual void SchedulePageLinks(CrawledPage crawledPage)
        {
            var linksToCrawl = 0;
            foreach (var hyperLink in crawledPage.ParsedLinks)
            {
                // First validate that the link was not already visited or added to the list of pages to visit, so we don't
                // make the same validation and fire the same events twice.
                if (!_scheduler.IsUriKnown(hyperLink.HrefValue) &&
                    (ShouldScheduleLinkDecisionMaker == null || ShouldScheduleLinkDecisionMaker.Invoke(hyperLink.HrefValue, crawledPage, _crawlContext)))
                {
                    try //Added due to a bug in the Uri class related to this (http://stackoverflow.com/questions/2814951/system-uriformatexception-invalid-uri-the-hostname-could-not-be-parsed)
                    {
                        var page = new PageToCrawl(hyperLink.HrefValue)
                        {
                            ParentUri = crawledPage.Uri,
                            CrawlDepth = crawledPage.CrawlDepth + 1,
                            IsInternal = IsInternalUri(hyperLink.HrefValue),
                            IsRoot = false
                        };

                        if (ShouldSchedulePageLink(page))
                        {
                            _scheduler.Add(page);
                            linksToCrawl++;
                        }

                        if (!ShouldScheduleMorePageLink(linksToCrawl))
                        {
                            _logger.Warn($"MaxLinksPerPage has been reached. No more links will be scheduled for current page [{crawledPage.Uri}].");
                            break;
                        }
                    }
                    catch { }
                }

                // Add this link to the list of known Urls so validations are not duplicated in the future.
                _scheduler.AddKnownUri(hyperLink.HrefValue);
            }
        }

        protected virtual bool ShouldSchedulePageLink(PageToCrawl page)
        {
            if ((page.IsInternal || _crawlContext.CrawlConfiguration.IsExternalPageCrawlingEnabled) && (ShouldCrawlPage(page)))
                return true;

            return false;   
        }

        protected virtual bool ShouldScheduleMorePageLink(int linksAdded)
        {
            return _crawlContext.CrawlConfiguration.MaxLinksPerPage == 0 || _crawlContext.CrawlConfiguration.MaxLinksPerPage > linksAdded;
        }

        protected virtual CrawlDecision ShouldDownloadPageContent(CrawledPage crawledPage)
        {
            var decision = _crawlDecisionMaker.ShouldDownloadPageContent(crawledPage, _crawlContext);
            if (decision.Allow)
                decision = (ShouldDownloadPageContentDecisionMaker != null) ? ShouldDownloadPageContentDecisionMaker.Invoke(crawledPage, _crawlContext) : new CrawlDecision { Allow = true };

            SignalCrawlStopIfNeeded(decision);
            return decision;
        }

        protected virtual void PrintConfigValues(CrawlConfiguration config)
        {
            _logger.Info("Configuration Values:");

            var indentString = new string(' ', 2);
            var abotVersion = Assembly.GetAssembly(this.GetType()).GetName().Version.ToString();
            _logger.Info($"{indentString}Abot Version: {abotVersion}");
            foreach (var property in config.GetType().GetProperties())
            {
                if (property.Name != "ConfigurationExtensions")
                    _logger.Info($"{indentString}{property.Name}: {property.GetValue(config, null)}");
            }

            foreach (var key in config.ConfigurationExtensions.Keys)
            {
                _logger.Info($"{indentString}{key}: {config.ConfigurationExtensions[key]}");
            }
        }

        protected virtual void SignalCrawlStopIfNeeded(CrawlDecision decision)
        {
            if (decision.ShouldHardStopCrawl)
            {
                _logger.Warn($"Decision marked crawl [Hard Stop] for site [{_crawlContext.RootUri}], [{decision.Reason}]");
                _crawlContext.IsCrawlHardStopRequested = decision.ShouldHardStopCrawl;
            }
            else if (decision.ShouldStopCrawl)
            {
                _logger.Warn($"Decision marked crawl [Stop] for site [{_crawlContext.RootUri}], [{decision.Reason}]");
                _crawlContext.IsCrawlStopRequested = decision.ShouldStopCrawl;
            }
        }

        protected virtual void WaitMinimumRetryDelay(PageToCrawl pageToCrawl)
        {
            //TODO No unit tests cover these lines
            if (pageToCrawl.LastRequest == null)
            {
                _logger.Warn($"pageToCrawl.LastRequest value is null for Url:{pageToCrawl.Uri.AbsoluteUri}. Cannot retry without this value.");
                return;
            }

            var milliSinceLastRequest = (DateTime.Now - pageToCrawl.LastRequest.Value).TotalMilliseconds;
            double milliToWait;
            if (pageToCrawl.RetryAfter.HasValue)
            {
                // Use the time to wait provided by the server instead of the config, if any.
                milliToWait = pageToCrawl.RetryAfter.Value*1000 - milliSinceLastRequest;
            }
            else
            {
                if (!(milliSinceLastRequest < _crawlContext.CrawlConfiguration.MinRetryDelayInMilliseconds)) return;
                milliToWait = _crawlContext.CrawlConfiguration.MinRetryDelayInMilliseconds - milliSinceLastRequest;
            }

            _logger.Info($"Waiting [{milliToWait}] milliseconds before retrying Url:[{pageToCrawl.Uri.AbsoluteUri}] LastRequest:[{pageToCrawl.LastRequest}] SoonestNextRequest:[{pageToCrawl.LastRequest.Value.AddMilliseconds(_crawlContext.CrawlConfiguration.MinRetryDelayInMilliseconds)}]");

            //TODO Cannot use RateLimiter since it currently cannot handle dynamic sleep times so using Thread.Sleep in the meantime
            if (milliToWait > 0)
                Thread.Sleep(TimeSpan.FromMilliseconds(milliToWait));
        }

        /// <summary>
        /// Validate that the Root page was not redirected. If the root page is redirected, we assume that the root uri
        /// should be changed to the uri where it was redirected.
        /// </summary>
        protected virtual void ValidateRootUriForRedirection(CrawledPage crawledRootPage)
        {
            if (!crawledRootPage.IsRoot) {
                throw new ArgumentException("The crawled page must be the root page to be validated for redirection.");
            }

            if (IsRedirect(crawledRootPage)) {
                _crawlContext.RootUri = ExtractRedirectUri(crawledRootPage);
                _logger.Info($"The root URI [{_crawlContext.OriginalRootUri}] was redirected to [{_crawlContext.RootUri}]. [{_crawlContext.RootUri}] is the new root.");
            }
        }

        /// <summary>
        /// Retrieve the URI where the specified crawled page was redirected.
        /// </summary>
        /// <remarks>
        /// If HTTP auto redirections is disabled, this value is stored in the 'Location' header of the response.
        /// If auto redirections is enabled, this value is stored in the response's ResponseUri property.
        /// </remarks>
        protected virtual Uri ExtractRedirectUri(CrawledPage crawledPage)
        {
            Uri locationUri;
            if (_crawlContext.CrawlConfiguration.IsHttpRequestAutoRedirectsEnabled) {
                // For auto redirects, look for the response uri.
                locationUri = crawledPage.HttpResponseMessage.RequestMessage.RequestUri;
            } else {
                // For manual redirects, we need to look for the location header.
                var location = crawledPage.HttpResponseMessage?.Headers?.Location?.AbsoluteUri;

                // Check if the location is absolute. If not, create an absolute uri.
                if (!Uri.TryCreate(location, UriKind.Absolute, out locationUri))
                {
                    var baseUri = new Uri(crawledPage.Uri.GetLeftPart(UriPartial.Authority));
                    locationUri = new Uri(baseUri, location);
                }
            }
            return locationUri;
        }

        #endregion
    }
}