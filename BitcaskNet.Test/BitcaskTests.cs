using System.IO;
using System.Text;
using Xunit;

namespace BitcaskNet.Test
{
    public class BitcaskTests
    {
        [Fact]
        public void PutThenGet()
        {
            var temporaryDirectory = GetTemporaryDirectory();
            var key = new byte[] { 0, 0 };
            var value = Encoding.Default.GetBytes("zero");

            using (var d = new Bitcask(temporaryDirectory))
            {
                d.Put(key, value);
                Assert.Equal(value, d.Get(key));
            }
        }

        [Fact]
        public void PutTwiceUpdatesTheValue()
        {
            using (var d = new Bitcask(GetTemporaryDirectory()))
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
            using (var d = new Bitcask(GetTemporaryDirectory()))
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
            var temporaryDirectory = GetTemporaryDirectory();
            var key = new byte[] { 0, 0 };
            var value = Encoding.Default.GetBytes("zero");

            using (var d = new Bitcask(temporaryDirectory))
            {
                d.Put(key, value);
            }

            using (var d = new Bitcask(temporaryDirectory))
            {
                Assert.Equal(value, d.Get(key));
            }
        }

        [Fact]
        public void RetrievingDeletedRecord()
        {
            var temporaryDirectory = GetTemporaryDirectory();
            var key1 = new byte[] { 0, 0 };
            var value1 = Encoding.Default.GetBytes("zero");
            var key2 = new byte[] { 1, 1 };
            var value2 = Encoding.Default.GetBytes("one");

            using (var d = new Bitcask(temporaryDirectory))
            {
                d.Put(key1, value1);
                d.Put(key2, value2);
                d.Delete(key1);
            }

            using (var d = new Bitcask(temporaryDirectory))
            {
                Assert.Null(d.Get(key1));
                Assert.Equal(value2, d.Get(key2));
            }
        }

        public string GetTemporaryDirectory()
        {
            string tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(tempDirectory);
            return tempDirectory;
        }
    }
}
