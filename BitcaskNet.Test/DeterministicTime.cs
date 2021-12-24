namespace BitcaskNet.Test
{
    internal class DeterministicTimeStrategy: ITimeStrategy
    {
        private long milliseconds = 0;

        public long GetUnixTimeMilliseconds()
        {
            return ++milliseconds;
        }

        public long GetUnixTimeSeconds()
        {
            milliseconds += 1000;
            return milliseconds/1000;
        }
    }
}
