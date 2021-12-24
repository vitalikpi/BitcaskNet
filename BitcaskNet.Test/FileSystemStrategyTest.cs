using System.IO;
using System.Linq;
using System.Threading;
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
            var (fileId, writer, _) = strategy.CreateActiveStreams();
            writer.Close();
            Assert.Equal(expectedFileList[0], Path.GetFileName(fileId));

            (fileId, writer, _) = strategy.CreateActiveStreams();
            writer.Close();
            Assert.Equal(expectedFileList[1], Path.GetFileName(fileId));

            (fileId, writer, _) = strategy.CreateActiveStreams();
            writer.Close();
            Assert.Equal(expectedFileList[2], Path.GetFileName(fileId));

            Assert.True(strategy.EnumerateFiles().Select(Path.GetFileName).ToArray().SequenceEqual(expectedFileList));
        }
    }
}
