namespace DBBackend.Shared
{
    public interface ITimeStrategy
    {
        long GetUnixTimeMilliseconds();
        long GetUnixTimeSeconds();
    }
}