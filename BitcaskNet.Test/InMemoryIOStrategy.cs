using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BitcaskNet.Test
{
    internal class InMemoryIOStrategy: IIOStrategy
    {
        private readonly Dictionary<string, byte[]> _directory;
        private char _lastUsedId = '`'; // The char before 'a'

        public InMemoryIOStrategy()
        {
            _directory = new Dictionary<string, byte[]>();
        }

        public IEnumerable<string> EnumerateFiles()
        {
            return _directory.Keys;
        }

        public Stream MakeReadStream(string fileId)
        {
            return new MemoryStream(_directory[fileId], false);
        }

        public (string activeFileId, Stream writeStream, Stream readStream) CreateActiveStreams()
        {
            var fileId = CreateFile();
            return (_lastUsedId.ToString(), new MemoryStream(_directory[fileId], false), new MemoryStream(_directory[fileId], true));
        }

        public string CreateFile()
        {
            if (_lastUsedId == 'z')
            {
                throw new NotImplementedException();
            }

            _lastUsedId = (char)(_lastUsedId + 1);
            var buffer = new byte[1024];
            _directory[_lastUsedId.ToString()] = buffer;

            return _lastUsedId.ToString();
        }

        public long AppendRecord(long position, string fileId, long timestamp, byte[] key, byte[] value)
        {
            using var stream =  new MemoryStream(_directory[fileId], true);
            stream.Seek(position, SeekOrigin.Begin);
            using var bw = new BinaryWriter(stream);

            bw.Write(timestamp);
            bw.Write(key.Length);
            bw.Write(value.Length);
            bw.Write(key);
            bw.Write(value);
            bw.Flush();

            return stream.Position;
        }

    }
}
