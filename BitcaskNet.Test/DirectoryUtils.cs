using System.IO;

namespace BitcaskNet.Test
{
    static class DirectoryUtils
    {
        public static string CreateTemporaryDirectory()
        {
            string tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(tempDirectory);
            return tempDirectory;
        }
    }
}
