using DBBackend.Shared;
using Murmur;
using Xunit;

namespace BitcaskNet.Test
{
    public class BitcaskKeyTests
    {
        private readonly Murmur128 _murmur;

        public BitcaskKeyTests()
        {
            _murmur = MurmurHash.Create128(seed: 3475832);
        }

        [Fact]
        public void HashConsistency()
        {
            var key = new BinaryArrayKey(new byte[] {0, 0}, _murmur);
            Assert.Equal(key.GetHashCode(), key.GetHashCode());
        }

        [Fact]
        public void Equality()
        {
            var key1 = new BinaryArrayKey(new byte[] { 0, 0 }, _murmur);
            var key1copy = new BinaryArrayKey(new byte[] { 0, 0 }, _murmur);
            var key2 = new BinaryArrayKey(new byte[] { 1, 1 }, _murmur);


            Assert.Equal(key1, key1);
            Assert.Equal(key1, key1copy);
            Assert.NotEqual(key1, key2);
            Assert.NotEqual(key1, new object());
        }
    }
}
