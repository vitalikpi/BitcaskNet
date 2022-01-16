using DBBackend.Interfaces;
using DBBackend.Shared;
using Microsoft.Extensions.Logging;
using Murmur;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;

namespace SSTableBackend
{
    internal class SSTableBackend : IDBBackend
    {
        private readonly HashAlgorithm _murmur;
        private readonly DataWriter _writer;        
        private SortedDictionary<BinaryArrayKey, byte[]> _memTable = new SortedDictionary<BinaryArrayKey, byte[]> ();
        private ILogger<SSTableBackend> _logger;
        private IIOStrategy _fileSystem;
        private readonly long _maxFileSize;

        public SSTableBackend()
        {
            _murmur = MurmurHash.Create128(seed: 3475832);
        }

        internal SSTableBackend(ILogger<SSTableBackend> logger, IIOStrategy ioStrategy, long maxFileSize)
        {
            _logger = logger;
            _fileSystem = ioStrategy;
            _maxFileSize = maxFileSize;

            _murmur = MurmurHash.Create128(seed: 3475832);

            _writer = _fileSystem.MakeWriter();
        }

        public void Put(byte[] key, byte[] value)
        {
            _memTable.Add(new BinaryArrayKey(key, _murmur), value);
        }

        public byte[] Get(byte[] key)
        {
            var binaryKey = new BinaryArrayKey(key, _murmur);
            if (_memTable.TryGetValue(binaryKey, out var value))
            {
                return value;
            }
            else
            {
                return null;
            }
        }

        public IEnumerable<byte[]> ListKeys()
        {
            throw new NotImplementedException();
        }

        public void Delete(byte[] key)
        {
            throw new NotImplementedException();
        }

        public void Merge()
        {
            throw new NotImplementedException();
        }

        public void Sync()
        {
            var old = _memTable;
            _memTable = new SortedDictionary<BinaryArrayKey, byte[]>();
            foreach (var key in old.Keys)
            {

            }
        }

        public void Close()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
