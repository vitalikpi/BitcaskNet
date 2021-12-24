namespace BitcaskNet
{
    internal interface ITimeStrategy
    {
        long GetUnixTimeMilliseconds();
        long GetUnixTimeSeconds();
    }
}