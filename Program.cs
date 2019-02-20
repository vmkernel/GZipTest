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

        // Read sequence number
        static long _readSequenceNumber;

        // Read sequence number
        static long _writeSequenceNumber;

        // Threads count according to CPU cores count
        static int _threadCount;

        // HARDCODE: size of a buffer per thread
        static int _chunkSize = 256 * 1024 * 1024; // 512M per thread

        // HARDCODE: name of source file to pack
        //static String _srcFileName = @"E:\Downloads\Movies\Mad.Max.Fury.Road.2015.1080p.BluRay.AC3.x264-ETRG.mkv";
        static String _srcFileName = @"D:\tmp\2016-02-03-raspbian-jessie.img";

        // Source file stream to read data from
        static FileStream _inputStream;

        // Source file stream to write data to
        static FileStream _outputStream;

        // 
        static bool _hasFileReadThreadExited;

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
        }

        // Threaded file read function
        static void InputFileReadThread(object parameter)
        {
            String fileName = (String)parameter;
            using (_inputStream = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            {
                int bytesRead;
                int bufferSize;
                Boolean hasStartedThread;

                while (_inputStream.Position < _inputStream.Length)
                {
                    // TODO: Add 'thread slot free' event from running threads if no thread has been started during current loop
                    hasStartedThread = false;

                    for (int threadIndex = 0; threadIndex < _threadCount; threadIndex++)
                    {
                        if (_threads[threadIndex] == null)
                        {
                            // read data
                            if ((_inputStream.Length - _inputStream.Position) < _chunkSize)
                            {
                                bufferSize = (int)(_inputStream.Length - _inputStream.Position);
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

                            bytesRead = _inputStream.Read(_threads[threadIndex].InputBuffer, 0, _threads[threadIndex].InputBuffer.Length);
                            if (bytesRead <= 0)
                            {
                                break; // Reached the end of the file
                            }

                            _threads[threadIndex].WorkerThread = new Thread(BlockCompressionThread);
                            _threads[threadIndex].SequenceNumber = _readSequenceNumber;
                            _threads[threadIndex].WorkerThread.Name = String.Format("Block compression (idx: {0}, seq: {1}", threadIndex, _readSequenceNumber);
                            _readSequenceNumber++;

                            _threads[threadIndex].WorkerThread.Start(threadIndex);
                            hasStartedThread = true;
                        }
                    }

                    // Waiting for a thread to exit
                    if (!hasStartedThread)
                    {
                        Thread.Sleep(1000);
                    }
                }

                _hasFileReadThreadExited = true;
            }
        }

        // Threaded file write function
        static void OutputFileWriteThread(object parameter)
        {
            String fileName = (String)parameter;

            using (_outputStream = new FileStream(fileName, FileMode.Create))
            {
                Boolean hasWrittenData;
                Boolean hasNoThreads;
                while (true) // implement a kill-switch
                {
                    hasWrittenData = false;
                    hasNoThreads = true;

                    for (int threadIndex = 0; threadIndex < _threadCount; threadIndex++)
                    {
                        if (_threads[threadIndex] != null)
                        {
                            hasNoThreads = false;
                        }

                        if (_threads[threadIndex] != null &&
                            _threads[threadIndex].WorkerThread != null &&
                            _threads[threadIndex].WorkerThread.ThreadState == ThreadState.Stopped)
                        {
                            if (_threads[threadIndex].SequenceNumber == _writeSequenceNumber)
                            {
                                _outputStream.Write(_threads[threadIndex].OutputBuffer, 0, _threads[threadIndex].OutputBuffer.Length);
                                _writeSequenceNumber++;
                                _threads[threadIndex] = null;
                                hasWrittenData = true;
                            }
                        }
                    }

                    if (!hasWrittenData)
                    {
                        Thread.Sleep(1000);
                    }

                    if (_hasFileReadThreadExited && hasNoThreads)
                    {
                        return;
                    }
                }
            }
        }

        static int Main(string[] args)
        {
            // Init local variables
            _threadCount = Environment.ProcessorCount;
            _threads = new GZipThread[_threadCount];
            _readSequenceNumber = 0;
            _writeSequenceNumber = 0;

            FileInfo srcFileInfo = new FileInfo(_srcFileName);
            String dstFileName = srcFileInfo.FullName + ".gz";

            Thread inputFileReadThread = new Thread(InputFileReadThread);
            inputFileReadThread.Name = "Read input file";
            inputFileReadThread.Start(_srcFileName);

            Thread outputFileWriteThread = new Thread(OutputFileWriteThread);
            outputFileWriteThread.Name = "Write output file";
            outputFileWriteThread.Start(dstFileName);

            Console.ReadLine();

            return 0;
        }
    }
}
