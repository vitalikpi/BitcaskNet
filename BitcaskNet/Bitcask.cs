using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Murmur;
using Force.Crc32;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using DBBackend.Interfaces;
using DBBackend.Shared;

namespace BitcaskNet
{
    public class Bitcask : IDisposable, IDBBackend
    {
        private object _activeFileLock = new object();
        private const string Thombstone = "Bitcask thombstone";
        private readonly ConcurrentDictionary<BinaryArrayKey, Block> _keydir = new ConcurrentDictionary<BinaryArrayKey, Block>();
        private readonly ILogger<Bitcask> _logger;
        private DataWriter _writer;
        private readonly HashAlgorithm _murmur;
        private readonly byte[] _thombstoneObject = MakeThombstone();

        private readonly IIOStrategy _fileSystem;
        private readonly ITimeStrategy _time;
        private readonly long _maxFileSize; // In bytes

        /// <summary>
        /// Open a new or existing Bitcask datastore with additional options.
        /// Valid options include read write(if this process is going to be a
        /// writer and not just a reader) and sync on put(if this writer would
        /// prefer to sync the write file after every write operation).
        /// The directory must be readable and writable by this process, and
        /// only one process may open a Bitcask with read write at a time.
        /// </summary>
        /// <param name="dirrectoryName"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public Bitcask(ILogger<Bitcask> logger, string directory)
            : this(logger, new FileSystemStrategy(directory, new TimeStrategy()), new TimeStrategy(), 1024 * 1024 * 1024)
        {
        }

        internal Bitcask(ILogger<Bitcask> logger, IIOStrategy ioStrategy, ITimeStrategy time, long maxFileSize)
        {
            _logger = logger;
            _fileSystem = ioStrategy;
            _time = time;
            _maxFileSize = maxFileSize;

            this._murmur = MurmurHash.Create128(seed: 3475832);

            LoadKeyDir();

            _writer = _fileSystem.MakeWriter();
        }

        private void LoadKeyDir()
        {
            foreach (var fileId in _fileSystem.EnumerateFiles())
            {
                var file = _fileSystem.MakeReader(fileId);

                foreach (var record in file.IterateOverRecords())
                {
                    var key = new BinaryArrayKey(record.keyBytes, this._murmur);

                    if (_keydir.ContainsKey(key) && record.timestamp < _keydir[key].Timestamp)
                    {
                        continue;
                    }

                    if (record.value.SequenceEqual(_thombstoneObject))
                    {
                        if (_keydir.ContainsKey(key))
                        {
                            _keydir.TryRemove(key, out var _);
                        }
                    }
                    else
                    {
                        var block = new Block
                        {
                            FileId = file.FileId,
                            ValueSize = record.value.Length,
                            ValuePos = record.valuePosition,
                            Timestamp = record.timestamp
                        };

                        _keydir[key] = block;
                    }
                }
            }
        }

        public byte[] Get(byte[] key)
        {
            var internalKey = new BinaryArrayKey(key, _murmur);

            if (_keydir.TryGetValue(internalKey, out Block block))
            {
                if (_writer.FileId == block.FileId)
                {
                    return _writer.ReadSingleValue(block.ValuePos, block.ValueSize);
                }
                else
                {
                    using var file = _fileSystem.MakeReader(block.FileId);
                    return file.ReadSingleValue(block.ValuePos, block.ValueSize);
                }
            }
            else
            {
                return null;
            }
        }

        public void Put(byte[] key, byte[] value)
        {
            AppendToActiveFileAndRunAction(key, value, (internalKey, block) => _keydir[internalKey] = block);
        }

        private void AppendToActiveFileAndRunAction(byte[] key, byte[] value, Action<BinaryArrayKey, Block> action)
        {
            var unixTimestamp = _time.GetUnixTimeMilliseconds();
            var valuePosition = _writer.Append(unixTimestamp, key, value);

            var block = new Block
            {
                FileId = this._writer.FileId,
                ValueSize = value.Length,
                ValuePos = valuePosition,
                Timestamp = unixTimestamp
            };

            action(new BinaryArrayKey(key, _murmur), block);

            lock (_activeFileLock)
            {
                if (_writer.BytesWritten > _maxFileSize)
                {
                    Sync();
                    _writer.Dispose();
                    _writer = _fileSystem.MakeWriter();
                }
            }
        }

        public void Delete(byte[] key)
        {
            AppendToActiveFileAndRunAction(key, _thombstoneObject, (internalKey, block) =>
            {
                if (_keydir.TryRemove(internalKey, out var _))
                    _logger.LogError("Thombstone was added to file but failed to remove from keydir.");
            });
        }

        private static byte[] MakeThombstone()
        {
            return Encoding.Default.GetBytes(Thombstone);
        }

        public IEnumerable<byte[]> ListKeys()
        {
            return _keydir.Keys.Select(c => c._array);
        }

        public void Fold()
        {
            throw new NotImplementedException();
        }

        public void Merge()
        {
            var files = _fileSystem.EnumerateFiles().Where(file => file != _writer.FileId);

            foreach (var fileId in files)
            {
                var reader = _fileSystem.MakeReader(fileId);
                foreach (var record in reader.IterateOverRecords())
                {
                    var key = new BinaryArrayKey(record.keyBytes, _murmur);
                    if (record.value.SequenceEqual(_thombstoneObject))
                    {
                        continue;
                    }

                    if (_keydir.ContainsKey(key)
                        && record.valueSize == _keydir[key].ValueSize
                        && Get(record.keyBytes).SequenceEqual(record.value))
                    {
                        Put(record.keyBytes, record.value);
                    }
                }
            }

            foreach (var fileId in files)
            {
                _fileSystem.DeleteFile(fileId);
            }
        }

        public void Sync()
        {
            _writer.Flush();
        }

        public void Close()
        {
            this.Dispose();
        }

        public void Dispose()
        {
            _writer.Dispose();
        }
    }
}
