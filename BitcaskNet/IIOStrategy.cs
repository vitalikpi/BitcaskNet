using System.Collections.Generic;
using System.IO;

namespace BitcaskNet
{
    internal interface IIOStrategy
    {
        IEnumerable<string> EnumerateFiles();
        Stream MakeReadStream(string fileId);
        (string activeFileId, Stream writeStream, Stream readStream) CreateActiveStreams();
        void DeleteFile(string fileId);
    }
}