using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace BitcaskNet
{
    internal class FileSystemStrategy : IIOStrategy
    {
        private readonly string _path;
        private readonly ITimeStrategy _time;

        public FileSystemStrategy(string path, ITimeStrategy time)
        {
            _path = path;
            _time = time;
        }

        public IEnumerable<string> EnumerateFiles()
        {
            return Directory
                .EnumerateFiles(_path, "*.bitcask.data")
                .OrderBy(path => long.Parse(Path.GetFileNameWithoutExtension(Path.GetFileNameWithoutExtension(path))));
        }

        public Stream MakeReadStream(string fileId)
        {
            return new FileStream(Path.Combine(_path, fileId) , FileMode.Open, FileAccess.Read, FileShare.Read);
        }

        public (string activeFileId, Stream writeStream, Stream readStream) CreateActiveStreams()
        {
            var activeFileId = Path.Combine(_path, _time.GetUnixTimeSeconds() + ".bitcask.data");
            var readStream = new FileStream(activeFileId, FileMode.OpenOrCreate, FileAccess.Read, FileShare.Write);
            var writeStream = new FileStream(activeFileId, FileMode.Append, FileAccess.Write, FileShare.Read);
            return (activeFileId, readStream, writeStream);
        }

        public void DeleteFile(string fileId)
        {
            File.Delete(fileId);
        }
    }
}
