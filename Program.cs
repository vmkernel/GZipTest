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
        private static Int32 s_chunkSize = 128 * 1024 * 1024; // 128M per thread

        // HARDCODE: name of source file to pack
        static String s_srcFileName = @"E:\Downloads\Movies\Imaginaerum.2012.1080p.BluRay.x264.YIFY.mp4";
        //static String s_srcFileName = @"E:\Downloads\Movies\Crazy.Stupid.Love.2011.1080p.MKV.AC3.DTS.Eng.NL.Subs.EE.Rel.NL.mkv";
        //static String _srcFileName = @"D:\tmp\2016-02-03-raspbian-jessie.img";
        //private static String s_srcFileName = @"D:\tmp\Iteration4-2x4CPU_16GB_RAM.blg";

        // Source file stream to read data from
        private static FileStream s_inputStream;

        // Source file stream to write data to
        private static FileStream s_outputStream;

        // Flag that file read thread has exited
        private static Boolean s_isFileReadThreadTerminated;

        // Flag that file write thread has exited
        private static Boolean s_isFileWriteThreadTerminated;

        // Input data queue
        private static Dictionary<Int64, Byte[]> s_readQueue;

        // Output data queue
        private static Dictionary<Int64, Byte[]> s_writeQueue;

        // Write buffer lock object
        private static readonly object s_writeQueueLocker = new Object();

        // Read buffer lock object
        private static readonly object s_readQueueLocker = new Object();

        // Threaded compression function
        private static void BlockCompressionThread(object parameter)
        {
            Int64 threadSequenceNumber = (Int64)parameter;
            byte[] buffer;
            
            lock (s_readQueueLocker)
            {
                buffer = s_readQueue[threadSequenceNumber];
                s_readQueue.Remove(threadSequenceNumber);
            }            

            lock (s_writeQueueLocker)
            {
                s_writeQueue.Add(threadSequenceNumber, buffer);
            }
            
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
            s_isFileReadThreadTerminated = false;

            using (s_inputStream = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            {
                Int64 bytesRead;
                Int64 bufferSize;

                while (s_inputStream.Position < s_inputStream.Length)
                {
                    // TODO: Add 'thread slot free' event from running threads if no thread has been started during current loop
                    // TODO: Read a block to internal buffer and wait for a free thread in order to speed up the whole process

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

                    lock (s_readQueueLocker)
                    {
                        s_readQueue.Add(s_readSequenceNumber, buffer);
                    }
                    
                    s_readSequenceNumber++;
                }

                s_isFileReadThreadTerminated = true;
            }
        }

        // Threaded file write function
        private static void OutputFileWriteThread(object parameter)
        {
            // TODO: what if I'll make a output queue (Dictionary<int, byte[]) to which all finished blocks will be copied 
            //  until their's turn comes (by it's write sequence number)
            String fileName = (String)parameter;
            s_isFileWriteThreadTerminated = false;

            using (s_outputStream = new FileStream(fileName, FileMode.Create))
            {
                Boolean hasWrittenData;
                while (true) // implement a kill-switch
                {
                    hasWrittenData = false;

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
                        byte[] buffer;
                        lock (s_writeQueueLocker)
                        {
                            buffer = s_writeQueue[s_writeSequenceNumber];
                            s_writeQueue.Remove(s_writeSequenceNumber);
                        }

                        s_outputStream.Write(buffer, 0, buffer.Length);
                        
                        s_writeSequenceNumber++;
                        hasWrittenData = true;
                    }

                    if (!hasWrittenData)
                    {
                        Thread.Sleep(1000);
                    }

                    if (s_isFileReadThreadTerminated && 
                        s_readQueue.Count <= 0 &&
                        s_writeQueue.Count <= 0)
                    {
                        s_isFileWriteThreadTerminated = true;
                        return;
                    }
                }
            }
        }

        // Threaded threads dispatcher that starts worker threads and limits their number
        private static void WorkerThreadsDispatcher(object parameter)
        {
            Boolean isWorkerThreadStarted;
            while (true) // TODO: implement a kill switch
            {
                isWorkerThreadStarted = false;

                if (s_threads.Count < s_maxThreadsCount &&
                    s_readQueue.Count > 0)
                {
                    Thread workerThread = new Thread(BlockCompressionThread);
                    // If there's less than maximum allowed threads are running, spawn a new one                    
                    lock (s_readQueueLocker)
                    {
                        foreach (Int64 threadSequenceNumber in s_readQueue.Keys)
                        {
                            if (!s_threads.ContainsKey(threadSequenceNumber))
                            {
                                workerThread.Name = String.Format("Block compression (seq: {0})", threadSequenceNumber);
                                s_threads.Add(threadSequenceNumber, workerThread);
                                s_threads[threadSequenceNumber].Start(threadSequenceNumber);
                                isWorkerThreadStarted = true;
                                break;
                            }
                        }
                    }
                }

                // Cleaning up finished threads
                List<Int64> threadsToRemove = new List<Int64>();
                foreach (Int64 threadSequenceNumber in s_threads.Keys)
                {
                    if (s_threads[threadSequenceNumber].ThreadState == ThreadState.Stopped ||
                        s_threads[threadSequenceNumber].ThreadState == ThreadState.Aborted)
                    {
                        threadsToRemove.Add(threadSequenceNumber);
                    }
                }
                foreach (Int64 threadSequenceNumber in threadsToRemove)
                {
                    s_threads.Remove(threadSequenceNumber);
                }

                // If read and write threads are terminated as well as read and write queues are empty
                if (s_isFileReadThreadTerminated &&
                    s_isFileWriteThreadTerminated &&
                    s_readQueue.Count <= 0 &&
                    s_writeQueue.Count <= 0)
                {
                    // stop the thread
                    return;
                }

                // Sleeping a little to not overload CPU
                if (!isWorkerThreadStarted && 
                    threadsToRemove.Count <= 0)
                {
                    Thread.Sleep(1000);
                }
            }
        }

        static int Main(string[] args)
        {
            // Init local variables
            //_threadCount = Environment.ProcessorCount;
            s_maxThreadsCount = 4;
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

            Thread workerThreadsManagerThread = new Thread(WorkerThreadsDispatcher);
            workerThreadsManagerThread.Name = "Worker threads manager";
            workerThreadsManagerThread.Start(null);

            Console.ReadLine();

            return 0;
        }
    }
}
