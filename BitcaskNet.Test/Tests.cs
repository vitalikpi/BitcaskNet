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

        public string GetTemporaryDirectory()
        {
            string tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(tempDirectory);
            return tempDirectory;
        }
    }
}
