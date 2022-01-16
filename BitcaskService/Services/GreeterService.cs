using DBBackend.Interfaces;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace BitcaskService
{
    public class GreeterService : Greeter.GreeterBase
    {
        private readonly ILogger<GreeterService> _logger;
        private readonly IDBBackend _bitcask;

        public GreeterService(ILogger<GreeterService> logger, IDBBackend bitcask)
        {
            _logger = logger;
            _bitcask = bitcask;
        }

        public override Task<Result> Delete(Key key, ServerCallContext context)
        {
            return Task.Run(() => {
                _bitcask.Delete(key.Key_.ToByteArray());
                return new Result() { Success = true };
            });
        }

        public override Task<Value> Get(Key key, ServerCallContext context)
        {
            return Task.Run(() => {
                var bytes = _bitcask.Get(key.Key_.ToByteArray());
                var byteString = Google.Protobuf.ByteString.CopyFrom(bytes); // TODO: is it really copying bytes?
                return new Value() { Bytes = byteString };
            });
        }

        public override Task<Result> Put(KeyValue request, ServerCallContext context)
        {
            return Task.Run(() => {
                _bitcask.Put(request.Key.ToByteArray(), request.Value.ToByteArray());
                return new Result() { Success = true };
            });
        }


        public override async Task ListKeys(Google.Protobuf.WellKnownTypes.Empty _, IServerStreamWriter<global::BitcaskService.Key> responseStream, ServerCallContext context)
        {
            foreach (var key in _bitcask.ListKeys())
            {
                var byteString = Google.Protobuf.ByteString.CopyFrom(key);
                await responseStream.WriteAsync(new Key { Key_ = byteString });
            }
        }
    }
}
