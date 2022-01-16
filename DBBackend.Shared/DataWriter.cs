using Force.Crc32;
using System;
using System.IO;

namespace DBBackend.Shared
{
    public class DataWriter : DataReader
    {
        private Stream _writeStream;
        private object _writeLock = new object();
        private BinaryWriter _bw;

        public DataWriter(string fileName, Stream readStream, Stream writeStream)
            : base(fileName, readStream)
        {
            _writeStream = writeStream;
            _bw = new BinaryWriter(this._writeStream);
        }

        public long BytesWritten => _writeStream.Position;

        public long Append(long unixTimestamp, byte[] key, byte[] value)
        {            
            uint intermediate = Crc32CAlgorithm.Compute(key);
            uint crc = Crc32CAlgorithm.Append(intermediate, value);
            long position;

            lock (_writeLock)
            {
                _bw.Write(crc);
                _bw.Write(unixTimestamp);
                _bw.Write(key.Length);
                _bw.Write(value.Length);
                _bw.Write(key);
                position = _writeStream.Position;
                _bw.Write(value);
                _bw.Flush();
            }

            return position;
        }

        public override void Dispose()
        {
            base.Dispose();
            if (_writeStream != null)
            {
                _writeStream.Dispose();
            }
        }

        public void Flush()
        {
            _writeStream.Flush();
        }
    }
}
