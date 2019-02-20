using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace GZipTest
{
    class GZipThread
    {
        byte[] _inputBuffer;
        public byte[] InputBuffer
        {
            get
            {
                return _inputBuffer;
            }
            set
            {
                _inputBuffer = value;
            }
        }

        byte[] _outputBuffer;
        public byte[] OutputBuffer
        {
            get
            {
                return _outputBuffer;
            }
            set
            {
                _outputBuffer = value;
            }
        }

        Thread _workerThread;
        public Thread WorkerThread
        {
            get
            {
                return _workerThread;
            }
            set
            {
                _workerThread = value;
            }
        }

        long _sequenceNumber;
        public long SequenceNumber
        {
            get
            {
                return _sequenceNumber;
            }
            set
            {
                _sequenceNumber = value;
            }
        }

        public GZipThread()
        {
            _sequenceNumber = -1;
        }
    }

    class Program
    {
        // Threads pool
        static GZipThread[] _threads;

        // Sequence number
        static long _sequenceNumber;

        // Threads count according to CPU cores count
        static int _threadCount;

        // Data buffer which is shared between threads
        static byte[][] _srcBuffer;

        // Destination data buffer which is shared between threads
        static byte[][] _dstBuffer;

        // Pool of threads
        //static Thread[] _threads;

        // HARDCODE: size of a buffer per thread
        static int _chunkSize = 512 * 1024 * 1024; // 512M per thread

        // HARDCODE: name of source file to pack
        static String _srcFileName = @"E:\Downloads\Movies\Mad.Max.Fury.Road.2015.1080p.BluRay.AC3.x264-ETRG.mkv";
        //static String _srcFileName = @"D:\tmp\Iteration4-2x4CPU_16GB_RAM.blg";

        // Source file stream to read data from
        static FileStream inputStream;

        // Destination file stream to read data from
        static FileStream _dstFileStream;

        // Threaded compression function
        static void BlockCompressionThread(object parameter)
        {
            int threadIndex = (int)parameter;
            using (MemoryStream outputStream = new MemoryStream(_threads[threadIndex].InputBuffer.Length))
            {
                using (GZipStream compressionStream = new GZipStream(outputStream, CompressionMode.Compress))
                {
                    compressionStream.Write(_threads[threadIndex].InputBuffer, 0, _threads[threadIndex].InputBuffer.Length);
                }
                _threads[threadIndex].OutputBuffer = outputStream.ToArray();
            }

            /*
            int threadIndex = (int)parameter;
            using (MemoryStream outputStream = new MemoryStream(_srcBuffer[threadIndex].Length))
            {
                using (GZipStream compressionStream = new GZipStream(outputStream, CompressionMode.Compress))
                {
                    compressionStream.Write(_srcBuffer[threadIndex], 0, _srcBuffer[threadIndex].Length);
                }
                _dstBuffer[threadIndex] = outputStream.ToArray();
            }
            */
        }

        // Threaded file read function
        static void InputFileReadThread(object parameter)
        {
            String fileName = (String)parameter;
            inputStream = new FileStream(fileName, FileMode.Open, FileAccess.Read);

            int bytesRead;
            int bufferSize;

            while (inputStream.Position < inputStream.Length)
            {
                // TODO: Add 'thread slot free' event from running threads if no thread has been started during current loop
                for (int threadIndex = 0; threadIndex < _threadCount; threadIndex++)
                {
                    if (_threads[threadIndex] == null)
                    {
                        // read data
                        if ((inputStream.Length - inputStream.Position) < _chunkSize)
                        {
                            bufferSize = (int)(inputStream.Length - inputStream.Position);
                        }
                        else
                        {
                            bufferSize = _chunkSize;
                        }

                        if (bufferSize <= 0)
                        {
                            break;
                        }

                        _threads[threadIndex] = new GZipThread();
                        _threads[threadIndex].InputBuffer = new byte[bufferSize];

                        bytesRead = inputStream.Read(_threads[threadIndex].InputBuffer, 0, _threads[threadIndex].InputBuffer.Length);
                        if (bytesRead <= 0)
                        {   
                            break; // Reached the end of the file
                        }

                        _threads[threadIndex].WorkerThread = new Thread(BlockCompressionThread);
                        _threads[threadIndex].SequenceNumber = _sequenceNumber;
                        _sequenceNumber++;

                        _threads[threadIndex].WorkerThread.Start(threadIndex);
                        
                    }
                }
            }
        }

        // Threaded file write function
        static void OutputFileWriteThread(object parameter)
        {/*
            String fileName = (String)parameter;

            _dstFileStream = new FileStream(fileName, FileMode.CreateNew);

            while (true) // implement a kill-switch
            {
                for (int threadIndex = 0; threadIndex < _threadCount; threadIndex++)
                {
                    if (_threads[threadIndex] != null && _threads[threadIndex].ThreadState == ThreadState.Stopped)
                    {
                        // check sequence number
                        // write data
                    }
                }
            }*/
        }

        static int Main(string[] args)
        {
            // Init local variables
            _threadCount = Environment.ProcessorCount;
            _threads = new GZipThread[_threadCount];
            _sequenceNumber = 0;

            FileInfo srcFileInfo = new FileInfo(_srcFileName);
            String dstFileName = srcFileInfo.FullName + ".gz";

            Thread inputFileReadThread = new Thread(InputFileReadThread);
            inputFileReadThread.Start(_srcFileName);

            /*
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
                                if (_threads[threadIndex] != null && _threads[threadIndex].ThreadState == ThreadState.Running)
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
            */
            Console.ReadLine();

            return 0;
        }
    }
}
