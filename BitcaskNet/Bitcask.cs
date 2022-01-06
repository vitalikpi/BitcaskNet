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

namespace BitcaskNet
{
    public class Bitcask: IDisposable
    {
        private object _writeLock = new object();
        private object _activeFileLock = new object();
        private const string Thombstone = "Bitcask thombstone";
        private readonly ConcurrentDictionary<BitcaskKey, Block> _keydir = new ConcurrentDictionary<BitcaskKey, Block>();
        private readonly ILogger<Bitcask> _logger;
        private string _activeFileId;
        private Stream _readStream;
        private Stream _writeStream;
        private BinaryWriter _bw;
        private readonly HashAlgorithm _murmur;
        private readonly byte[] _thombstoneObject = MakeThombstone();
        
        private readonly IIOStrategy _fileSystem;
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
            : this(logger, new FileSystemStrategy(directory, new TimeStrategy()), 1024*1024*1024)
        {
        }

        internal Bitcask(ILogger<Bitcask> logger, IIOStrategy ioStrategy, long maxFileSize)
        {
            _logger = logger;
            _fileSystem = ioStrategy;
            _maxFileSize = maxFileSize;

            this._murmur = MurmurHash.Create128(seed: 3475832);

            LoadKeyDir();

            (this._activeFileId, this._readStream, this._writeStream) = _fileSystem.CreateActiveStreams();
            this._bw = new BinaryWriter(this._writeStream);
        }

        private void LoadKeyDir()
        {
            foreach (var record in IterateOverRecords(_fileSystem.EnumerateFiles()))
            {
                var key = new BitcaskKey(record.keyBytes, this._murmur);

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
                        FileId = record.fileId,
                        ValueSize = record.value.Length,
                        ValuePos = record.valuePosition,
                        Timestamp = record.timestamp
                    };

                    _keydir[key] = block;
                }
            }
        }

        private IEnumerable<(string fileId, BitcaskKey key, long timestamp, int keySize, int valueSize, byte[] keyBytes, long valuePosition, byte[] value)> IterateOverRecords(IEnumerable<string> files)
        {
            foreach (var fileId in files)
            {
                using var rs = _fileSystem.MakeReadStream(fileId);
                using var reader = new BinaryReader(rs);

                while (true)
                {
                    long recordPosition;
                    uint expectedCRC;
                    long timestamp;
                    int keySize;
                    int valueSize;
                    byte[] keyBytes;
                    long valuePosition;
                    byte[] value;

                    try
                    {
                        recordPosition = rs.Position;
                        expectedCRC = reader.ReadUInt32();
                        timestamp = reader.ReadInt64();
                        keySize = reader.ReadInt32();
                        valueSize = reader.ReadInt32();
                        keyBytes = reader.ReadBytes(keySize);
                        valuePosition = rs.Position;
                        value = reader.ReadBytes(valueSize);
                    }
                    catch (EndOfStreamException)
                    {
                        // Intentionally swallow this exception
                        yield break;
                    }


                    uint intermediate = Crc32CAlgorithm.Compute(keyBytes);
                    uint actualCRC = Crc32CAlgorithm.Append(intermediate, value);

                    if (actualCRC != expectedCRC)
                    {
                        _logger.LogError("CRC mismatch error fileid=[" + fileId +"], position=[" + recordPosition + "]");
                        continue;
                    }

                    var key = new BitcaskKey(keyBytes, this._murmur);

                    yield return (fileId, key, timestamp, keySize, valueSize, keyBytes, valuePosition, value);
                }
            }
        }

        public byte[] Get(byte[] key)
        {
            var internalKey = new BitcaskKey(key, _murmur);

            if (_keydir.TryGetValue(internalKey, out Block block))
            {
                if (_activeFileId == block.FileId)
                {
                    _readStream.Seek(block.ValuePos, SeekOrigin.Begin);
                    var buf = new byte[block.ValueSize];
                    _readStream.Read(buf, 0, block.ValueSize);

                    return buf;
                }
                else
                {
                    using var rs = _fileSystem.MakeReadStream(block.FileId);
                    using var reader = new BinaryReader(rs);
                    rs.Seek(block.ValuePos, SeekOrigin.Begin);
                    var value = reader.ReadBytes(block.ValueSize);
                    return value;
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

        private void AppendToActiveFileAndRunAction(byte[] key, byte[] value, Action<BitcaskKey,Block> action)
        {
            var timestamp = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds(); // TODO: make sure that this should be milliseconds
            uint intermediate = Crc32CAlgorithm.Compute(key);
            uint crc = Crc32CAlgorithm.Append(intermediate, value);
            var internalKey = new BitcaskKey(key, _murmur);

            lock (_writeLock)
            {
                _bw.Write(crc);
                _bw.Write(timestamp);
                _bw.Write(key.Length);
                _bw.Write(value.Length);
                _bw.Write(key);
                var position = _writeStream.Position;
                _bw.Write(value);
                _bw.Flush();

                var block = new Block
                {
                    FileId = this._activeFileId,
                    ValueSize = value.Length,
                    ValuePos = position,
                    Timestamp = timestamp
                };

                action(internalKey, block);
            }

            lock (_activeFileLock)
            {
                if (_writeStream.Position > _maxFileSize)
                {
                    Sync();
                    _bw.Dispose();
                    _writeStream.Dispose();
                    _readStream.Dispose();

                    (this._activeFileId, this._readStream, this._writeStream) = _fileSystem.CreateActiveStreams();
                    _bw = new BinaryWriter(this._writeStream);
                }
            }
        }

        public void Delete(byte[] key)
        {
            AppendToActiveFileAndRunAction(key, _thombstoneObject, (internalKey, block) => {
                if (_keydir.TryRemove(internalKey, out var _))
                    _logger.LogError("Tho,bstone was added to file but failed to remove from keydir.");
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
            var files = _fileSystem.EnumerateFiles().Where(fileid => fileid != _activeFileId);

            foreach (var record in IterateOverRecords(files))
            {
                if (record.value.SequenceEqual(_thombstoneObject))
                {
                    continue;
                }

                if (_keydir.ContainsKey(record.key)
                    && record.valueSize == _keydir[record.key].ValueSize
                    && Get(record.keyBytes).SequenceEqual(record.value) )
                {
                    Put(record.keyBytes, record.value);
                }
            }

            foreach (var fileId in files)
            {
                _fileSystem.DeleteFile(fileId);
            }
        }

        public void Sync()
        {
            _bw.Flush();
        }

        public void Close()
        {
            this.Dispose();
        }

        public void Dispose()
        {
            _bw.Flush();
            this._readStream.Close();
            this._bw.Close();
            this._writeStream.Close();
        }
    }
}
