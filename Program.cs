using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace GZipTest
{
    class CGZipThread
    {
        private Byte[] _inputBuffer;
        public Byte[] InputBuffer
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

        private Byte[] _outputBuffer;
        public Byte[] OutputBuffer
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

        private Thread _workerThread;
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

        private Int64 _sequenceNumber;
        public Int64 SequenceNumber
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

        public CGZipThread()
        {
            _sequenceNumber = -1;
        }
    }

    class Program
    {
        // Threads pool
        //private static CGZipThread[] s_threads;
        private static Dictionary<Int64, Thread> s_threads;

        // Read sequence number
        private static Int64 s_readSequenceNumber;

        // Read sequence number
        private static Int64 s_writeSequenceNumber;

        // Threads count according to CPU cores count
        private static Int32 s_maxThreadsCount;

        // HARDCODE: size of a buffer per thread
        private static Int32 s_chunkSize = 512 * 1024 * 1024; // 512M per thread

        // HARDCODE: name of source file to pack
        //static String _srcFileName = @"E:\Downloads\Movies\Crazy.Stupid.Love.2011.1080p.MKV.AC3.DTS.Eng.NL.Subs.EE.Rel.NL.mkv";
        //static String _srcFileName = @"D:\tmp\2016-02-03-raspbian-jessie.img";
        private static String s_srcFileName = @"D:\tmp\Iteration4-2x4CPU_16GB_RAM.blg";

        // Source file stream to read data from
        private static FileStream s_inputStream;

        // Source file stream to write data to
        private static FileStream s_outputStream;

        // Flag that file read thread has exited
        private static Boolean s_hasFileReadThreadExited;

        // Input data queue
        private static Dictionary<Int64, Byte[]> s_readQueue;

        // Output data queue
        private static Dictionary<Int64, Byte[]> s_writeQueue;

        // Write buffer lock object
        private static readonly object s_writeQueueLocker = new Object();

        // Threaded compression function
        private static void BlockCompressionThread(object parameter)
        {
            /*
            Int32 threadIndex = (Int32)parameter;
            using (MemoryStream outputStream = new MemoryStream(s_threads[threadIndex].InputBuffer.Length))
            {
                using (GZipStream compressionStream = new GZipStream(outputStream, CompressionMode.Compress))
                {
                    compressionStream.Write(s_threads[threadIndex].InputBuffer, 0, s_threads[threadIndex].InputBuffer.Length);
                }

                lock (s_writeQueueLocker)
                {
                    s_writeQueue.Add(s_threads[threadIndex].SequenceNumber, outputStream.ToArray());
                }

                // Marking this thread as free
                s_threads[threadIndex].SequenceNumber = -1;
            }
            */
        }

        // Threaded file read function
        private static void InputFileReadThread(object parameter)
        {
            String fileName = (String)parameter;
            using (s_inputStream = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            {
                Int64 bytesRead;
                Int64 bufferSize;
                Boolean hasStartedThread;

                while (s_inputStream.Position < s_inputStream.Length)
                {
                    // TODO: Add 'thread slot free' event from running threads if no thread has been started during current loop
                    // TODO: Read a block to internal buffer and wait for a free thread in order to speed up the whole process
                    hasStartedThread = false;

                    // read data
                    if ((s_inputStream.Length - s_inputStream.Position) < s_chunkSize)
                    {
                        bufferSize = (Int32)(s_inputStream.Length - s_inputStream.Position);
                    }
                    else
                    {
                        bufferSize = s_chunkSize;
                    }

                    if (bufferSize <= 0)
                    {
                        break;
                    }

                    Byte[] buffer = new Byte[bufferSize];
                    bytesRead = s_inputStream.Read(buffer, 0, buffer.Length);
                    if (bytesRead <= 0)
                    {
                        break; // Reached the end of the file (not required)
                    }

                    s_readQueue.Add(s_readSequenceNumber, buffer);
                    s_readSequenceNumber++;
                    buffer = null;

                    /*
                    do
                    {
                        for (Int32 threadIndex = 0; threadIndex < s_threadCount; threadIndex++)
                        {
                            if (_threads[threadIndex] == null ||
                                _threads[threadIndex].SequenceNumber == -1)
                            {
                                _threads[threadIndex] = new CGZipThread();
                                _threads[threadIndex].WorkerThread = new Thread(BlockCompressionThread);
                                _threads[threadIndex].SequenceNumber = s_readSequenceNumber;
                                _threads[threadIndex].WorkerThread.Name = String.Format("Block compression (idx: {0}, seq: {1}", threadIndex, s_readSequenceNumber);
                                _threads[threadIndex].InputBuffer = buffer;
                                _threads[threadIndex].WorkerThread.Start(threadIndex);

                                
                                hasStartedThread = true;
                                s_readSequenceNumber++;
                                break;
                            }
                        }

                        if (!hasStartedThread)
                        {
                            Thread.Sleep(1000);
                        }

                    } while (!hasStartedThread);
                    */
                }

                lock (s_writeQueueLocker)
                {
                    s_writeQueue = s_readQueue;
                }                
                s_readQueue = null;
                s_hasFileReadThreadExited = true;
            }
        }

        // Threaded file write function
        private static void OutputFileWriteThread(object parameter)
        {
            // TODO: what if I'll make a output queue (Dictionary<int, byte[]) to which all finished blocks will be copied 
            //  until their's turn comes (by it's write sequence number)
            String fileName = (String)parameter;

            using (s_outputStream = new FileStream(fileName, FileMode.Create))
            {
                Boolean hasWrittenData;
                Boolean hasNoThreads;
                while (true) // implement a kill-switch
                {
                    hasWrittenData = false;
                    hasNoThreads = true;

                    Boolean isContainsWriteSequenceNumber = false;
                    Int32 writeBufferItemsCount = 0;

                    lock (s_writeQueueLocker)
                    {
                        writeBufferItemsCount = s_writeQueue.Count;
                        isContainsWriteSequenceNumber = s_writeQueue.ContainsKey(s_writeSequenceNumber);
                    }

                    if (writeBufferItemsCount > 0 && isContainsWriteSequenceNumber)
                    {
                        // Free up compression thread resources 
                        //  to allow file reader function to read a new chunk of data 
                        //  without waiting for compressed block to be written out
                        byte[] buffer = s_writeQueue[s_writeSequenceNumber];

                        s_outputStream.Write(buffer, 0, buffer.Length);
                        //_outputStream.Write(_writeBuffer[_writeSequenceNumber], 0, _writeBuffer[_writeSequenceNumber].Length);

                        lock (s_writeQueueLocker)
                        {
                            s_writeQueue.Remove(s_writeSequenceNumber);
                        }
                        
                        s_writeSequenceNumber++;
                        hasWrittenData = true;
                    }

                    /*
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
                                // Free up compression thread resources 
                                //  to allow file reader function to read a new chunk of data 
                                //  without waiting for compressed block to be written out
                                byte[] buffer = _threads[threadIndex].OutputBuffer;
                                _threads[threadIndex] = null;

                                _outputStream.Write(buffer, 0, buffer.Length);
                                _writeSequenceNumber++;
                                
                                hasWrittenData = true;
                            }
                        }
                    }
                    */

                    if (!hasWrittenData)
                    {
                        Thread.Sleep(1000);
                    }

                    if (s_hasFileReadThreadExited && 
                        hasNoThreads && 
                        s_writeQueue.Count <= 0)
                    {
                        return;
                    }
                }
            }
        }

        private static void WorkerThreadsManager(object parameter)
        {
            Boolean isThreadStarted;
            while (true) // TODO: implement a kill switch
            {
                isThreadStarted = false;

                if (s_threads.Count <= s_maxThreadsCount)
                {
                    // If there's less than maximum allowed threads are running, spawn a new one

                    Thread workerThread = new Thread(BlockCompressionThread);
                    workerThread.Name = String.Format("Block compression (seq: {0})", s_readSequenceNumber);
                    s_threads.Add(s_readSequenceNumber, workerThread);
                    s_threads[s_readSequenceNumber].Start(s_readSequenceNumber);
                    isThreadStarted = true;
                }

                if (!isThreadStarted)
                {
                    Thread.Sleep(1000);
                }
            }
        }

        static int Main(string[] args)
        {
            // Init local variables
            //_threadCount = Environment.ProcessorCount;
            s_maxThreadsCount = 2;
            //s_threads = new CGZipThread[s_threadCount];
            s_threads = new Dictionary<Int64, Thread>();
            s_readQueue = new Dictionary<Int64, Byte[]>();
            s_writeQueue = new Dictionary<Int64, Byte[]>();
            s_readSequenceNumber = 0;
            s_writeSequenceNumber = 0;

            FileInfo srcFileInfo = new FileInfo(s_srcFileName);
            String dstFileName = srcFileInfo.FullName + ".gz";

            Thread inputFileReadThread = new Thread(InputFileReadThread);
            inputFileReadThread.Name = "Read input file";
            inputFileReadThread.Start(s_srcFileName);

            Thread outputFileWriteThread = new Thread(OutputFileWriteThread);
            outputFileWriteThread.Name = "Write output file";
            outputFileWriteThread.Start(dstFileName);

            Thread workerThreadsManagerThread = new Thread(WorkerThreadsManager);
            workerThreadsManagerThread.Name = "Worker threads manager";
            workerThreadsManagerThread.Start(null);

            Console.ReadLine();

            return 0;
        }
    }
}
