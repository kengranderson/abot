using System;

namespace Abot2.Poco
{
    public class HyperLink
    {
        public Uri HrefValue { get; set; }

        public override string ToString() => HrefValue != null ? HrefValue.ToString() : base.ToString();
    }
}
