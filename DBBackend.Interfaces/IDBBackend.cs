using System.Collections.Generic;

namespace DBBackend.Interfaces
{
    public interface IDBBackend
    {
        void Put(byte[] key, byte[] value);
        byte[] Get(byte[] key);
        IEnumerable<byte[]> ListKeys();
        void Delete(byte[] key);

        void Merge();
        void Sync();

        void Close();
        void Dispose();
    }
}