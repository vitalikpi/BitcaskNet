using System.Text;
using Xunit;

namespace BitcaskNet.Test
{
    public class CorruptedFileTests
    {
        private readonly InMemoryIOStrategy _ds;

        public CorruptedFileTests()
        {
            _ds = new InMemoryIOStrategy();
        }

        [Fact]
        public void IncorrectOrderOfRecords()
        {
            var fileId = _ds.CreateFile();
            var key1 = new byte[] {1, 2, 3};
            var value1 = new byte[] {2,2,2};
            var updatedValue1 = new byte[] {3,3,3};

            long position = 0;
            position = _ds.AppendRecord(position, fileId, 1, key1, updatedValue1);
            _ds.AppendRecord(position, fileId, 0, key1, value1);

            using var bcsk = new Bitcask(_ds, 1024*1024*1024);


            Assert.Equal(updatedValue1, bcsk.Get(key1));
        }
    }
}
