using Microsoft.Extensions.Logging;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace BitcaskNet.Test
{
    public class BitcaskTests
    {
        private readonly ITestOutputHelper _output;
        private readonly ILogger<Bitcask> _logger;

        public BitcaskTests(ITestOutputHelper output)
        {
            _output = output;
            _logger = output.BuildLoggerFor<Bitcask>();
        }

        [Fact]
        public void PutThenGet()
        {
            var temporaryDirectory = DirectoryUtils.CreateTemporaryDirectory();
            var key = new byte[] { 0, 0 };
            var value = Encoding.Default.GetBytes("zero");

            using (var d = new Bitcask(_logger, temporaryDirectory))
            {
                d.Put(key, value);
                Assert.Equal(value, d.Get(key));
            }
        }

        [Fact]
        public void PutTwiceUpdatesTheValue()
        {
            using (var d = new Bitcask(_logger, DirectoryUtils.CreateTemporaryDirectory()))
            {
                var original = Encoding.Default.GetBytes("zero");
                var updated = Encoding.Default.GetBytes("nil");
                d.Put(new byte[] { 0, 0 }, original);
                d.Put(new byte[] { 0, 0 }, updated);
                Assert.Equal(updated, d.Get(new byte[] { 0, 0 }));
            }
        }

        [Fact]
        public void DeleteRemovesTheKey()
        {
            using (var d = new Bitcask(_logger, DirectoryUtils.CreateTemporaryDirectory()))
            {
                var key = new byte[] { 0, 0 };
                var value = Encoding.Default.GetBytes("zero");
                
                d.Put(key, value);
                d.Delete(key);
                
                Assert.Null(d.Get(key));
            }
        }

        [Fact]
        public void ReadFileAndRetrieveValue()
        {
            var temporaryDirectory = DirectoryUtils.CreateTemporaryDirectory();
            var key = new byte[] { 0, 0 };
            var value = Encoding.Default.GetBytes("zero");

            using (var d = new Bitcask(_logger, temporaryDirectory))
            {
                d.Put(key, value);
            }

            using (var d = new Bitcask(_logger, temporaryDirectory))
            {
                Assert.Equal(value, d.Get(key));
            }
        }

        [Fact]
        public void RetrievingDeletedRecord()
        {
            var temporaryDirectory = DirectoryUtils.CreateTemporaryDirectory();
            var key1 = new byte[] { 0, 0 };
            var value1 = Encoding.Default.GetBytes("zero");
            var key2 = new byte[] { 1, 1 };
            var value2 = Encoding.Default.GetBytes("one");

            using (var d = new Bitcask(_logger, temporaryDirectory))
            {
                d.Put(key1, value1);
                d.Put(key2, value2);
                d.Delete(key1);
            }

            using (var d = new Bitcask(_logger, temporaryDirectory))
            {
                Assert.Null(d.Get(key1));
                Assert.Equal(value2, d.Get(key2));
            }
        }

        [Fact]
        public void Merge()
        {
            var temporaryDirectory = DirectoryUtils.CreateTemporaryDirectory();

            using (var d = new Bitcask(_logger, temporaryDirectory))
            {
                d.Put(new byte[] { 1 }, new byte[] { 1, 1, 1 });
            }

            using (var d = new Bitcask(_logger, temporaryDirectory))
            {
                d.Put(new byte[] { 1 }, new byte[] { 2, 2, 2 });
            }

            using (var bcsk = new Bitcask(_logger, temporaryDirectory))
            {
                bcsk.Merge();
                Assert.Equal(new byte[] { 2, 2, 2 }, bcsk.Get(new byte[] { 1 }));
            }

            Assert.True(Directory.GetFiles(temporaryDirectory).Single().Any());

            using (var bcsk = new Bitcask(_logger, temporaryDirectory))
            {
                Assert.Equal(new byte[] { 2, 2, 2 }, bcsk.Get(new byte[] { 1 }));
            }
        }

        [Fact]
        public void MaxFileSize()
        {
            var temporaryDirectory = DirectoryUtils.CreateTemporaryDirectory();

            using (var d = new Bitcask(_logger, new FileSystemStrategy(temporaryDirectory, new DeterministicTimeStrategy()), 1024))
            {
                d.Put(new byte[] { 1 }, new byte[1025]);
                d.Put(new byte[] { 1 }, new byte[100]);
            }

            Assert.Equal(2, Directory.EnumerateFiles(temporaryDirectory, "*.bitcask.data").Count());
        }

    }
}
