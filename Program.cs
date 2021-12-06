using System;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace temp_artemis_producer
{
    class Program
    {
        private const String Address = "amqp://guest:guest@localhost:5672";
        static async Task Main(string[] args)
        {
            var cts = new CancellationTokenSource();

            var producerA = new MessageProducer( Address, "producerA", "Udai_Created_3", cts.Token );
            await producerA.StartSendingMessages();
            cts.Cancel();
            // await Task.WhenAll(producerATask);
        }
    }
    public sealed class MessageProducer
    {
        private readonly String _address;
        private readonly CancellationToken _cancellationToken;
        private readonly String _destination;
        private readonly String _producerName;

        public MessageProducer( String address, String producerName, String destination, CancellationToken cancellationToken )
        {
            _address = address;
            _producerName = producerName;
            _destination = destination;
            _cancellationToken = cancellationToken;
        }

        public async Task StartSendingMessages()
        {
            while ( !_cancellationToken.IsCancellationRequested )
            {
                var connectionFactory = new ConnectionFactory();
                var address = new Address( _address );
                var messageCounter = 0;
                try
                {
                    var connection = await connectionFactory.CreateAsync( address );
                    var session = ( (IConnection) connection ).CreateSession();
                    var sender = session.CreateSender( "Udai_Any_Name",
                                                       new Target
                                                       {
                                                           Address = _destination,
                                                           Capabilities = new[] { new Symbol( "queue" ) }
                                                       } );

                    while ( !_cancellationToken.IsCancellationRequested )
                    {
                        var message = new Message( $"Message #{messageCounter} from {_producerName}." )
                        {
                            Properties = new()
                            {
                                MessageId = $"{_producerName} #{messageCounter}"
                            }
                        };

                        await sender.SendAsync( message );
                        Console.WriteLine( $"\t{_producerName} - Sent message with id: '{message.Properties.MessageId}'" );

                        await Task.Delay( 1000, CancellationToken.None );
                        messageCounter++;
                    }
                }
                catch ( Exception ex )
                {
                    Console.WriteLine( $"{_producerName} - Connection error in producer '{_producerName}' {ex.Message} => create new connection." );
                    await Task.Delay( 1000, CancellationToken.None );
                }
            }
        }
    }
}
