using System;
using System.Diagnostics;
using NLog;

namespace Abot2.Util
{
    public interface IMemoryMonitor : IDisposable
    {
        int GetCurrentUsageInMb();
    }

    public class GcMemoryMonitor : IMemoryMonitor
    {
        protected static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        public virtual int GetCurrentUsageInMb()
        {
            var timer = Stopwatch.StartNew();
            var currentUsageInMb = Convert.ToInt32(GC.GetTotalMemory(false) / (1024 * 1024));
            timer.Stop();

            _logger.Debug($"GC reporting [{currentUsageInMb}mb] currently thought to be allocated, took [{timer.ElapsedMilliseconds}] millisecs");

            return currentUsageInMb;       
        }

        public void Dispose()
        {
            //do nothing
        }
    }
}
