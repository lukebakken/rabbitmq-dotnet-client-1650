using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace RabbitMQExample
{
    public static class Program
    {
        private static ManualResetEventSlim _exitLatch = new ManualResetEventSlim();
        private static Exception _appdomainException;

        static void Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;

            var cf = new ConnectionFactory();

            using (var c = cf.CreateConnection())
            {
                c.CallbackException += OnConnectionCallbackException;

                using (var m = c.CreateModel())
                {
                    m.CallbackException += OnModelCallbackException;

                    QueueDeclareOk queue = m.QueueDeclare();

                    var consumer = new EventingBasicConsumer(m);

                    consumer.Received += OnConsumerReceived;

                    m.BasicConsume(queue.QueueName, autoAck: false, consumer);

                    LogInfo("consuming from queue: {0}", queue.QueueName);

                    _exitLatch.Wait();
                }
            }

            if (_appdomainException != null)
            {
                throw _appdomainException;
            }
        }

        private static void OnConnectionCallbackException(object sender, CallbackExceptionEventArgs e)
        {
            LogError("connection received exception: {0}", e.Exception);
            _appdomainException = e.Exception;
            _exitLatch.Set();
        }

        private static void OnConsumerReceived(object sender, BasicDeliverEventArgs e)
        {
            LogInfo("received message: {0}", Encoding.ASCII.GetString(e.Body.ToArray()));
            throw new Exception("KABOOM");
        }

        private static void OnModelCallbackException(object sender, CallbackExceptionEventArgs e)
        {
            LogError("channel received exception: {0}", e.Exception);
            _appdomainException = e.Exception;
            _exitLatch.Set();
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            ErrorExit("current domain unhandled exception: {0}", e.ExceptionObject);
        }

        static void LogError(string format, params object[] args)
        {
            string message = string.Format(format, args);
            Console.Error.WriteLine("{0} [ERROR] {1}", DateTime.Now, message);
        }

        static void LogInfo(string format, params object[] args)
        {
            string message = string.Format(format, args);
            Console.WriteLine("{0} [INFO] {1}", DateTime.Now, message);
        }

        static void ErrorExit(string format, params object[] args)
        {
            LogError(format, args);
            Environment.Exit(1);
        }
    }
}