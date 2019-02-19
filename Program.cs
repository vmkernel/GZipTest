using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace GZipTest
{
    class Program
    {
        // Threads count according to CPU cores count
        static int _threadCount;

        // Data buffer which is shared between threads
        static byte[][] _srcBuffer;

        // Destination data buffer which is shared between threads
        static byte[][] _dstBuffer;

        // Pool of threads
        static Thread[] _threads;

        // HARDCODE: size of a buffer per thread
        static int _chunkSize = 512 * 1024 * 1024; // 512M per thread

        // HARDCODE: name of source file to pack
        static String _srcFileName = @"D:\tmp\Iteration4-2x4CPU_16GB_RAM.blg";


        // Threaded compression function
        static public void BlockCompressionThread(object parameter)
        {
            int threadIndex = (int)parameter;
            using (MemoryStream outputStream = new MemoryStream(_srcBuffer[threadIndex].Length))
            {
                using (GZipStream compressionStream = new GZipStream(outputStream, CompressionMode.Compress))
                {
                    compressionStream.Write(_srcBuffer[threadIndex], 0, _srcBuffer[threadIndex].Length);
                }
                _dstBuffer[threadIndex] = outputStream.ToArray();
            }
        }


        static int Main(string[] args)
        {
            // Init local variables
            _threads = new Thread[_threadCount];
            _threadCount = Environment.ProcessorCount;
            
            FileInfo srcFileInfo = new FileInfo(_srcFileName);
            String dstFileName = srcFileInfo.FullName + ".gz";

            int bytesRead;
            int bufferSize;
            using (FileStream sourceStream = new FileStream(_srcFileName, FileMode.Open, FileAccess.Read))
            {
                using (FileStream destinationStream = new FileStream(dstFileName, FileMode.Create))
                {
                    while (sourceStream.Position < sourceStream.Length)
                    {
                        _srcBuffer = new byte[_threadCount][];
                        _dstBuffer = new byte[_threadCount][];

                        for (int threadIndex = 0; threadIndex < _threadCount; threadIndex++)
                        {
                            if ((sourceStream.Length - sourceStream.Position) < _chunkSize)
                            {
                                bufferSize = (int)(sourceStream.Length - sourceStream.Position);
                            }
                            else
                            {
                                bufferSize = _chunkSize;
                            }

                            if (bufferSize <= 0)
                            {
                                break;
                            }

                            _srcBuffer[threadIndex] = new byte[bufferSize];
                            bytesRead = sourceStream.Read(_srcBuffer[threadIndex], 0, bufferSize);

                            _threads[threadIndex] = new Thread(BlockCompressionThread);
                            _threads[threadIndex].Start(threadIndex);
                        }

                        // Waiting for all threads to exit
                        Boolean bAllThreadsFinished;
                        do
                        {
                            bAllThreadsFinished = true;
                            for (int threadIndex = 0; threadIndex < _threadCount; threadIndex++)
                            {
                                if (_threads[threadIndex].ThreadState == ThreadState.Running)
                                {
                                    bAllThreadsFinished = false;
                                }
                            }

                        } while (!bAllThreadsFinished);

                        // Writing results to the output file
                        for (int threadIndex = 0; threadIndex < _threadCount; threadIndex++)
                        {
                            if ( _dstBuffer[threadIndex] != null && _dstBuffer[threadIndex].Length > 0)
                            {
                                destinationStream.Write(_dstBuffer[threadIndex], 0, _dstBuffer[threadIndex].Length);
                            }
                        }
                    } // Advancing to the next block of data until the end of the source file

                } // using destinationStream

            } // using sourceStream

            Console.ReadLine();

            return 0;
        }
    }
}
