using System;

namespace BitcaskNet
{
    [Flags]
    public enum Options
    {
        Read = 0x1,
        Write = 0x2,
        ReadWrite = Read | Write,
        SyncOnPut,
    }
}