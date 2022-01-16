using System;
using System.Linq;
using System.Security.Cryptography;

namespace DBBackend.Shared
{
    public class BinaryArrayKey
    {
        public readonly byte[] _array;
        private readonly HashAlgorithm _hashAlgorithm;

        public BinaryArrayKey(byte[] array, HashAlgorithm hashAlgorithm)
        {
            _array = array;
            _hashAlgorithm = hashAlgorithm;
        }

        public override int GetHashCode()
        {
            return BitConverter.ToInt32(_hashAlgorithm.ComputeHash(_array));
        }

        public override bool Equals(object other)
        {
            return Equals(other as BinaryArrayKey);
        }

        public bool Equals(BinaryArrayKey other)
        {
            if (other == null)
                return false;

            if (ReferenceEquals(this, other))
                return true;

            if (_array.Length != other._array.Length)
                return false;

            return _array.SequenceEqual(other._array);
        }
    }
}