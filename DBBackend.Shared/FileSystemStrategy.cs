using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DBBackend.Shared
{
    public class FileSystemStrategy : IIOStrategy
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

        public DataReader MakeReader(string fileId)
        {
            return new DataReader(fileId,
                new FileStream(Path.Combine(_path, fileId), FileMode.Open, FileAccess.Read, FileShare.Read));
        }

        public DataWriter MakeWriter()
        {
            var activeFileId = Path.Combine(_path, _time.GetUnixTimeSeconds() + ".bitcask.data");
            var readStream = new FileStream(activeFileId, FileMode.OpenOrCreate, FileAccess.Read, FileShare.Write);
            var writeStream = new FileStream(activeFileId, FileMode.Append, FileAccess.Write, FileShare.Read);
            return new DataWriter(activeFileId, readStream, writeStream);
        }

        public void DeleteFile(string fileId)
        {
            File.Delete(fileId);
        }
    }
}
