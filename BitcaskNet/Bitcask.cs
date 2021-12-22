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

        private Dictionary<BitcaskKey, Block> keydir = new Dictionary<BitcaskKey, Block>();
        private readonly string activeFilePath;
        private readonly FileStream readStream;
        private FileStream writeStream;
        private BinaryWriter bw;
        private readonly HashAlgorithm murmur;
        private byte[] _thombstoneObject = MakeThombstone();

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
        {
            this.murmur = MurmurHash.Create128(seed: 3475832);

            foreach (var file in Directory.EnumerateFiles(directory, "*.bitcask"))
            {
                using var rs = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.None);
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

                        var key = new BitcaskKey(keyBytes, this.murmur);

                        if (value.SequenceEqual(_thombstoneObject))
                        {
                            if (keydir.ContainsKey(key))
                            {
                                keydir.Remove(key);
                            }
                        }
                        else
                        {
                            var block = new Block
                            {
                                FileId = file,
                                ValueSize = value.Length,
                                ValuePos = valuePosition,
                                Timestamp = timestamp
                            };

                            keydir[key] = block;
                        }
                    }
                    catch (EndOfStreamException e)
                    {
                        // Intentionally swallow this exception
                        break;
                    }
                }
            }


            this.activeFilePath = Path.Combine(directory, Path.GetRandomFileName() + ".bitcask");
            this.readStream = new FileStream(activeFilePath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.Write);
            this.writeStream = new FileStream(this.activeFilePath, FileMode.Append, FileAccess.Write, FileShare.Read);
            this.bw = new BinaryWriter(this.writeStream);
            
        }

        public byte[] Get(byte[] key)
        {
            var internalKey = new BitcaskKey(key, murmur);

            if (!this.keydir.ContainsKey(internalKey))
            {
                return null;
            }

            var block = keydir[internalKey];

            if (activeFilePath == block.FileId)
            {
                readStream.Seek(block.ValuePos, SeekOrigin.Begin);
                var buf = new byte[block.ValueSize];
                readStream.Read(buf, 0, block.ValueSize);

                return buf;
            }
            else
            {
                using var rs = new FileStream(block.FileId, FileMode.Open, FileAccess.Read, FileShare.None);
                using var reader = new BinaryReader(rs);
                rs.Seek(block.ValuePos, SeekOrigin.Begin);
                var value = reader.ReadBytes(block.ValueSize);
                return value;
            }
        }

        public void Put(byte[] key, byte[] value)
        {
            var timestamp = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
            bw.Write(timestamp);
            bw.Write(key.Length);
            bw.Write(value.Length);
            bw.Write(key);
            var position = writeStream.Position;
            bw.Write(value);
            bw.Flush();

            var internalKey = new BitcaskKey(key, murmur);

            keydir[internalKey] = new Block
            {
                FileId = this.activeFilePath,
                ValueSize = value.Length,
                ValuePos = position,
                Timestamp = timestamp
            };
        }

        public void Delete(byte[] key)
        {
            var internalKey = new BitcaskKey(key, murmur);
            Put(key, _thombstoneObject);
            this.keydir.Remove(internalKey);
        }

        private static byte[] MakeThombstone()
        {
            return Encoding.Default.GetBytes(Thombstone);
        }

        public IEnumerable<byte[]> ListKeys()
        {
            return keydir.Keys.Select(c => c._array);
        }

        public void Fold()
        {
            throw new NotImplementedException();
        }

        public void Merge()
        {
            throw new NotImplementedException();
        }

        public void Sync()
        {
            bw.Flush();
        }

        public void Close()
        {
            this.Dispose();
        }

        public void Dispose()
        {
            bw.Flush();
            this.readStream.Close();
            this.bw.Close();
            this.writeStream.Close();
        }
    }
}
