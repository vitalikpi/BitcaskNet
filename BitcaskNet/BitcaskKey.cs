using System;
using System.Linq;
using System.Security.Cryptography;

namespace BitcaskNet
{
    public class BitcaskKey
    {
        internal readonly byte[] _array;
        private readonly HashAlgorithm _murmur;

        public BitcaskKey(byte[] array, HashAlgorithm murmur)
        {
            _array = array;
            _murmur = murmur;
        }

        public override int GetHashCode()
        {
            return BitConverter.ToInt32(_murmur.ComputeHash(_array));
        }

        public override bool Equals(object? other)
        {
            return Equals(other as BitcaskKey);
        }

        public bool Equals(BitcaskKey other)
        {
            if (other == null)
                return false;

            if (object.ReferenceEquals(this, other))
                return true;

            if (this._array.Length != other._array.Length)
                return false;

            return _array.SequenceEqual(other._array);
        }
    }
}