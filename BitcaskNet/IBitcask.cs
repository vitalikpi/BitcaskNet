using System.Collections.Generic;

namespace BitcaskNet
{
    public interface IBitcask
    {
        void Close();
        void Delete(byte[] key);
        void Dispose();
        void Fold();
        byte[] Get(byte[] key);
        IEnumerable<byte[]> ListKeys();
        void Merge();
        void Put(byte[] key, byte[] value);
        void Sync();
    }
}