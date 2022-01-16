using DBBackend.Shared;
using System.IO;
using System.Linq;
using Xunit;

namespace BitcaskNet.Test
{
    public class FileSystemStrategyTest
    {
        [Fact]
        public void Test()
        {
            var expectedFileList = new[] {"1.bitcask.data", "2.bitcask.data", "3.bitcask.data"};
            var directoryPath = DirectoryUtils.CreateTemporaryDirectory();

            var strategy = new FileSystemStrategy(directoryPath, new DeterministicTimeStrategy());
            var writer = strategy.MakeWriter();
            writer.Dispose();
            Assert.Equal(expectedFileList[0], Path.GetFileName(writer.FileId));

            writer  = strategy.MakeWriter();
            writer.Dispose();
            Assert.Equal(expectedFileList[1], Path.GetFileName(writer.FileId));

            writer = strategy.MakeWriter();
            writer.Dispose();
            Assert.Equal(expectedFileList[2], Path.GetFileName(writer.FileId));

            Assert.True(strategy.EnumerateFiles().Select(fileId => Path.GetFileName(fileId)).ToArray().SequenceEqual(expectedFileList));
        }
    }
}
