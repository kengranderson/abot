using System;
using System.Timers;
using NLog;

namespace Abot2.Util
{
    public class CachedMemoryMonitor : IMemoryMonitor, IDisposable
    {
        protected static readonly Logger _logger = LogManager.GetCurrentClassLogger();
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
            var oldUsage = _cachedCurrentUsageInMb;
            _cachedCurrentUsageInMb = _memoryMonitor.GetCurrentUsageInMb();
            _logger.Debug($"Updated cached memory usage value from [{oldUsage}mb] to [{_cachedCurrentUsageInMb}mb]");
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
