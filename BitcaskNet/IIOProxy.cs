using System.Collections.Generic;
using System.IO;

namespace BitcaskNet
{
    public interface IIOProxy
    {
        IEnumerable<string> EnumerateFiles();
        Stream MakeReadStream(string fileId);
        (string activeFileId, Stream writeStream, Stream readStream) CreateActiveStreams();
    }
}