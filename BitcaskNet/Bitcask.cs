using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Murmur;

namespace BitcaskNet
{
    public class Bitcask: IDisposable
    {
        private const string Thombstone = "Bitcask thombstone";

        private readonly Dictionary<BitcaskKey, Block> _keydir = new Dictionary<BitcaskKey, Block>();
        private readonly string _activeFileId;
        private readonly Stream _readStream;
        private readonly Stream _writeStream;
        private readonly BinaryWriter _bw;
        private readonly HashAlgorithm _murmur;
        private readonly byte[] _thombstoneObject = MakeThombstone();
        private readonly IIOStrategy _fileSystem;
        private readonly ITimeStrategy _time;

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
        public Bitcask(string directory)
            : this(new FileSystemStrategy(directory, new TimeStrategy()), new TimeStrategy())
        {
        }

        internal Bitcask(IIOStrategy ioStrategy, ITimeStrategy time)
        {
            _fileSystem = ioStrategy;
            _time = time;

            this._murmur = MurmurHash.Create128(seed: 3475832);


            foreach (var fileId in _fileSystem.EnumerateFiles())
            {
                using var rs = _fileSystem.MakeReadStream(fileId);
                using var reader = new BinaryReader(rs);

                while (true)
                {
                    try
                    {
                        var timestamp = reader.ReadInt64();
                        var keySize = reader.ReadInt32();
                        var valueSize = reader.ReadInt32();
                        var keyBytes = reader.ReadBytes(keySize);
                        var valuePosition = rs.Position;
                        var value = reader.ReadBytes(valueSize);

                        var key = new BitcaskKey(keyBytes, this._murmur);

                        if (_keydir.ContainsKey(key) && timestamp < _keydir[key].Timestamp)
                        {
                            continue;
                        } 

                        if (value.SequenceEqual(_thombstoneObject))
                        {
                            if (_keydir.ContainsKey(key))
                            {
                                _keydir.Remove(key);
                            }
                        }
                        else
                        {
                            var block = new Block
                            {
                                FileId = fileId,
                                ValueSize = value.Length,
                                ValuePos = valuePosition,
                                Timestamp = timestamp
                            };

                            _keydir[key] = block;
                        }
                    }
                    catch (EndOfStreamException)
                    {
                        // Intentionally swallow this exception
                        break;
                    }
                }
            }

            (this._activeFileId, this._readStream, this._writeStream) = _fileSystem.CreateActiveStreams();
            this._bw = new BinaryWriter(this._writeStream);
        }

        private IEnumerable<(BitcaskKey key, long timestamp, int keySize, int valueSize, byte[] keyBytes, long valuePosition, byte[] value)> IterateOverRecords(IEnumerable<string> files)
        {
            foreach (var fileId in files)
            {
                using var rs = _fileSystem.MakeReadStream(fileId);
                using var reader = new BinaryReader(rs);

                while (true)
                {
                    long timestamp;
                    int keySize;
                    int valueSize;
                    byte[] keyBytes;
                    long valuePosition;
                    byte[] value;


                    try
                    {
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

                    var key = new BitcaskKey(keyBytes, this._murmur);

                    yield return (key, timestamp, keySize, valueSize, keyBytes, valuePosition, value);
                }
            }
        }

        public byte[] Get(byte[] key)
        {
            var internalKey = new BitcaskKey(key, _murmur);

            if (!_keydir.ContainsKey(internalKey))
            {
                return null;
            }

            var block = _keydir[internalKey];

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

        public void Put(byte[] key, byte[] value)
        {
            var timestamp = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds(); // TODO: make sure that this should be milliseconds
            _bw.Write(timestamp);
            _bw.Write(key.Length);
            _bw.Write(value.Length);
            _bw.Write(key);
            var position = _writeStream.Position;
            _bw.Write(value);
            _bw.Flush();

            var internalKey = new BitcaskKey(key, _murmur);

            _keydir[internalKey] = new Block
            {
                FileId = this._activeFileId,
                ValueSize = value.Length,
                ValuePos = position,
                Timestamp = timestamp
            };
        }

        public void Delete(byte[] key)
        {
            var internalKey = new BitcaskKey(key, _murmur);
            Put(key, _thombstoneObject);
            this._keydir.Remove(internalKey);
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
