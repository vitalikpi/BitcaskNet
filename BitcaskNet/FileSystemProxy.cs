using System.Collections.Generic;
using System.IO;

namespace BitcaskNet
{
    internal class FileSystemProxy : IIOProxy
    {
        private readonly string _path;

        public FileSystemProxy(string path)
        {
            _path = path;
        }

        public IEnumerable<string> EnumerateFiles()
        {
            return Directory.EnumerateFiles(_path, "*.bitcask");
        }

        public Stream MakeReadStream(string fileId)
        {
            return new FileStream(Path.Combine(_path, fileId) , FileMode.Open, FileAccess.Read, FileShare.None);
        }

        public (string activeFileId, Stream writeStream, Stream readStream) CreateActiveStreams()
        {
            var activeFileId = Path.Combine(_path, Path.GetRandomFileName() + ".bitcask");
            var readStream = new FileStream(activeFileId, FileMode.OpenOrCreate, FileAccess.Read, FileShare.Write);
            var writeStream = new FileStream(activeFileId, FileMode.Append, FileAccess.Write, FileShare.Read);
            return (activeFileId, readStream, writeStream);
        }
    }
}
