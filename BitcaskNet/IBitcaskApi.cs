using System.Collections.Generic;

namespace BitcaskNet
{
    public interface IBitcaskApi
    {
        /// <summary>
        /// Open a new or existing Bitcask datastore with additional options.
        /// Valid options include read write(if this process is going to be a
        /// writer and not just a reader) and sync on put(if this writer would
        /// prefer to sync the write file after every write operation).
        /// The directory must be readable and writable by this process, and
        /// only one process may open a Bitcask with read write at a time.
        /// </summary>
        /// <param name="dirrectoryName"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        //IBitcaskHandle Open(string dirrectoryName, Options options);


        /// <summary>
        /// Open a new or existing Bitcask datastore for read-only access.
        /// The directory and all files in it must be readable by this process.
        /// </summary>
        /// <param name="dirrectoryName"></param>
        /// <returns></returns>
        //IBitcaskHandle Open(string dirrectoryName);

        /// <summary>
        /// Retrieve a value by key from a Bitcask datastore.
        /// </summary>
        object Get(object key);

        /// <summary>
        /// Store a key and value in a Bitcask datastore.
        /// </summary>
        void Put(object key, object value);

        /// <summary>
        /// Delete a key from a Bitcask datastore.
        /// </summary>
        void Delete();

        /// <summary>
        /// List all keys in a Bitcask datastore.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<object> ListKeys();

        /// <summary>
        /// Fold over all K/V pairs in a Bitcask datastore.
        /// Fun is expected to be of the form: F(K, V, Acc0) → Acc.
        /// </summary>
        public void Fold();

        /// <summary>
        /// Merge several data files within a Bitcask datastore into a more
        /// compact form.Also, produce hintfiles for faster startup
        /// </summary>
        public void Merge();

        /// <summary>
        /// Force any writes to sync to disk.
        /// </summary>
        public void Sync();

        /// <summary>
        /// Close a Bitcask data store and flush all pending writes
        /// (if any) to disk.
        /// </summary>
        public void Close();
    }
}