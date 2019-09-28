﻿using System;
using System.Runtime;
using NLog;

namespace Abot2.Util
{
    /// <summary>
    /// Handles memory monitoring/usage
    /// </summary>
    public interface IMemoryManager : IMemoryMonitor, IDisposable
    {
        /// <summary>
        /// Whether the current process that is hosting this instance is allocated/using above the param value of memory in mb
        /// </summary>
        bool IsCurrentUsageAbove(int sizeInMb);

        /// <summary>
        /// Whether there is at least the param value of available memory in mb
        /// </summary>
        bool IsSpaceAvailable(int sizeInMb);
    }

    public class MemoryManager : IMemoryManager
    {
        protected static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        readonly IMemoryMonitor _memoryMonitor;

        public MemoryManager(IMemoryMonitor memoryMonitor)
        {
            _memoryMonitor = memoryMonitor ?? throw new ArgumentNullException("memoryMonitor");
        }

        public virtual bool IsCurrentUsageAbove(int sizeInMb)
        {
            return GetCurrentUsageInMb() > sizeInMb;
        }

        public virtual bool IsSpaceAvailable(int sizeInMb)
        {
            if (sizeInMb < 1)
                return true;

            var isAvailable = true;

            MemoryFailPoint _memoryFailPoint = null;
            try
            {
                _memoryFailPoint = new MemoryFailPoint(sizeInMb);
            }
            catch (InsufficientMemoryException)
            {
                isAvailable = false;
            }
            catch (NotImplementedException)
            {
                _logger.Warn("MemoryFailPoint is not implemented on this platform. The MemoryManager.IsSpaceAvailable() will just return true.");
            }
            finally
            {
                if (_memoryFailPoint != null)
                    _memoryFailPoint.Dispose();
            }

            return isAvailable;
        }

        public virtual int GetCurrentUsageInMb()
        {
            return _memoryMonitor.GetCurrentUsageInMb();
        }

        public void Dispose()
        {
            _memoryMonitor.Dispose();
        }
    }
}
