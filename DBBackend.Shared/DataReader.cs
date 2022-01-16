using Force.Crc32;
using System;
using System.Collections.Generic;
using System.IO;

namespace DBBackend.Shared
{
    public class DataReader: IDisposable
    {
        protected readonly Stream _readStream;

        public DataReader(string fileName, Stream readStream)
        {
            FileId = fileName;
            _readStream = readStream;
        }

        public string FileId { get; }

        public virtual void Dispose()
        {
            if (_readStream != null)
            {
                _readStream.Dispose();
            }
        }

        public IEnumerable<(long timestamp, int keySize, int valueSize, byte[] keyBytes, long valuePosition, byte[] value)> IterateOverRecords()
        {
            using var reader = new BinaryReader(_readStream);

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
                    recordPosition = _readStream.Position;
                    expectedCRC = reader.ReadUInt32();
                    timestamp = reader.ReadInt64();
                    keySize = reader.ReadInt32();
                    valueSize = reader.ReadInt32();
                    keyBytes = reader.ReadBytes(keySize);
                    valuePosition = _readStream.Position;
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
                    //todo:_logger.LogError("CRC mismatch error fileid=[" + fileId + "], position=[" + recordPosition + "]");
                    continue;
                }

                //var key = new BinaryArrayKey(keyBytes, this._hashAlgorithm);

                yield return (timestamp, keySize, valueSize, keyBytes, valuePosition, value);
            }
        }

        public byte[] ReadSingleValue(long valuePosition, int valueSize)
        {
            using var reader = new BinaryReader(_readStream);
            _readStream.Seek(valuePosition, SeekOrigin.Begin);
            var value = reader.ReadBytes(valueSize);
            return value;
        }
    }
}
