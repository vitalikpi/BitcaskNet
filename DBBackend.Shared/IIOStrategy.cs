using System.Collections.Generic;

namespace DBBackend.Shared
{
    public interface IIOStrategy
    {
        IEnumerable<string> EnumerateFiles();
        DataReader MakeReader(string fileId);
        DataWriter MakeWriter();
        void DeleteFile(string fileId);
    }
}