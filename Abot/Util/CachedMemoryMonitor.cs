using log4net;
using System;
using System.Timers;

namespace Abot.Util
{
    [Serializable]
    public class CachedMemoryMonitor : IMemoryMonitor, IDisposable
    {
        static readonly ILog _logger = LogManager.GetLogger("AbotLogger");
        readonly IMemoryMonitor _memoryMonitor;
        readonly Timer _usageRefreshTimer;
        int _cachedCurrentUsageInMb;

        public CachedMemoryMonitor(IMemoryMonitor memoryMonitor, int cacheExpirationInSeconds)
        {
            if (cacheExpirationInSeconds < 1)
                cacheExpirationInSeconds = 5;

            _memoryMonitor = memoryMonitor ?? throw new ArgumentNullException("memoryMonitor");

            UpdateCurrentUsageValue();

            _usageRefreshTimer = new Timer(cacheExpirationInSeconds * 1000);
            _usageRefreshTimer.Elapsed += (sender, e) => UpdateCurrentUsageValue();
            _usageRefreshTimer.Start();
        }

        protected virtual void UpdateCurrentUsageValue()
        {
            int oldUsage = _cachedCurrentUsageInMb;
            _cachedCurrentUsageInMb = _memoryMonitor.GetCurrentUsageInMb();
            _logger.DebugFormat("Updated cached memory usage value from [{0}mb] to [{1}mb]", oldUsage, _cachedCurrentUsageInMb);
        }

        public virtual int GetCurrentUsageInMb()
        {
            return _cachedCurrentUsageInMb;
        }

        public void Dispose()
        {
            _usageRefreshTimer.Stop();
            _usageRefreshTimer.Dispose();
        }
    }
}
