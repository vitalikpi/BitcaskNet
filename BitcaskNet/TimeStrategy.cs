using System;

namespace BitcaskNet
{
    internal class TimeStrategy : ITimeStrategy
    {
        public long GetUnixTimeMilliseconds()
        {
            return new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
        }

        public long GetUnixTimeSeconds()
        {
            return new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds();
        }
    }
}
