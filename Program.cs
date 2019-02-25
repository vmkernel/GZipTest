using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace GZipTest
{

    class CGZipCompressor
    {

    }

    public struct SFileReadThreadArguments
    {
        public String fileName;
        public CompressionMode compressionMode;
    };

    class Program
    {
        // HARDCODE: size of a buffer per thread
        private static readonly Int32 s_chunkSize = 128 * 1024 * 1024; // 128M per thread

        // HARDCODE: name of source file to pack
        //private static readonly String s_srcFileName = @"E:\Downloads\Movies\Imaginaerum.2012.1080p.BluRay.x264.YIFY.mp4";
        private static readonly String s_srcFileName = @"E:\Downloads\Movies\Imaginaerum.2012.1080p.BluRay.x264.YIFY.mp4.gz";
        //private static readonly String s_srcFileName = @"E:\Downloads\Movies\Mad.Max.Fury.Road.2015.1080p.BluRay.AC3.x264-ETRG.mkv";
        //private static readonly String s_srcFileName = @"E:\Downloads\Movies\Crazy.Stupid.Love.2011.1080p.MKV.AC3.DTS.Eng.NL.Subs.EE.Rel.NL.mkv";
        //private static readonly String s_srcFileName = @"D:\tmp\2016-02-03-raspbian-jessie.img";
        //private static readonly String s_srcFileName = @"c:\tmp\Iteration4-2x4CPU_16GB_RAM.blg";
        //private static readonly String s_srcFileName = @"C:\tmp\uncompressed-file.zip";

        // Threads pool
        private static Dictionary<Int64, Thread> s_workerThreads;

        // Read sequence number
        private static Int64 s_readSequenceNumber;

        // Read sequence number
        private static Int64 s_writeSequenceNumber;

        // Write sequence number lock object
        private static readonly Object s_writeSequenceNumberLocker = new Object();

        // Threads count according to CPU cores count
        private static Int32 s_maxThreadsCount;

        // Maximum lenght of write queue. After reaching this value file read will be suspended until write queue will be sorten below this value
        private static Int32 s_maxWriteQueueLength;

        // Source file stream to read data from
        private static FileStream s_inputStream;

        // Source file stream to write data to
        private static FileStream s_outputStream;

        // Flag that file read thread has exited
        private static Boolean s_isInputFileRead;

        // Flags that all input data has been processed and all worker threads are terminated
        private static bool s_isDataProcessingDone;

        // Flag that file write thread has exited
        private static Boolean s_isOutputFileWritten;

        // Input data queue
        private static Dictionary<Int64, Byte[]> s_inputDataQueue;

        // Write buffer lock object
        private static readonly Object s_inputDataQueueLocker = new Object();

        // Output data queue
        private static Dictionary<Int64, Byte[]> s_outputDataQueue;
        
        // Read buffer lock object
        private static readonly Object s_outputDataQueueLocker = new Object();

        // Event that fires when output data queue is ready to receive new block of processed data
        private static ManualResetEvent s_signalOutputDataQueueReady = new ManualResetEvent(false);

        // Event that fires when data processing thread finished processing its block of data
        private static ManualResetEvent s_signalOutputDataReady = new ManualResetEvent(false);

        // TODO: Add a comment
        private static EventWaitHandle s_signalInputDataReady = new EventWaitHandle(false, EventResetMode.AutoReset);

        // TODO: Add a comment
        private static readonly Object s_outputDataReadySignalLocker = new Object();

        // TODO: Add a comment
        private static ManualResetEvent s_signalWorkerThreadExited = new ManualResetEvent(false);

        // TODO: Add a comment
        private static readonly Object s_WorkerTreadExitedSignalLocker = new Object();

        // Threaded file read function
        private static void FileReadThread(object parameter)
        {
            String fileName = (String)parameter;
            s_isInputFileRead = false;

            using (FileStream inputStream = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            {
                Int64 bytesRead;
                Int64 bufferSize;

                while (inputStream.Position < inputStream.Length)
                {
                    // Throtling read in order to allow the file writer thread to write out some data
                    s_signalOutputDataQueueReady.WaitOne();

                    // Reading data
                    if ((inputStream.Length - inputStream.Position) < s_chunkSize)
                    {
                        bufferSize = (Int32)(inputStream.Length - inputStream.Position);
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
                    bytesRead = inputStream.Read(buffer, 0, buffer.Length);
                    if (bytesRead <= 0)
                    {
                        break; // Reached the end of the file (not required)
                    }

                    lock (s_inputDataQueueLocker)
                    {
                        s_inputDataQueue.Add(s_readSequenceNumber, buffer);
                    }

                    s_readSequenceNumber++;
                    s_signalInputDataReady.Set();
                }

                s_isInputFileRead = true;
            }
        }


        private static void CompressedFileReadThread(object parameter)
        {
            String fileName = (String)parameter;
            s_isInputFileRead = false;

            Int64 bytesRead;
            Int64 bufferSize;

            using (FileStream inputStream = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            {
                using (GZipStream gZipStream = new GZipStream(inputStream, CompressionMode.Decompress))
                {
                    while (inputStream.Position < inputStream.Length)
                    {
                        // Throtling read in order to allow the file writer thread to write out some data
                        s_signalOutputDataQueueReady.WaitOne();

                        // Reading data
                        if ((inputStream.Length - inputStream.Position) < s_chunkSize)
                        {
                            bufferSize = (Int32)(inputStream.Length - inputStream.Position);
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
                        bytesRead = gZipStream.Read(buffer, 0, buffer.Length);
                        if (bytesRead <= 0)
                        {
                            break; // Reached the end of the file (not required)
                        }

                        lock (s_outputDataQueueLocker)
                        {
                            s_outputDataQueue.Add(s_readSequenceNumber, buffer);
                        }

                        s_readSequenceNumber++;
                        s_signalOutputDataReady.Set();
                    }
                }
            }

            s_isInputFileRead = true;
        }

        // Threaded file write function
        private static void FileWriteThread(object parameter)
        {
            String fileName = (String)parameter;

            s_isOutputFileWritten = false;

            using (s_outputStream = new FileStream(fileName, FileMode.Create))
            {
                // Initial command to file read thread to start reading
                s_signalOutputDataQueueReady.Set();

                while (true) // TODO: implement a kill-switch
                {
                    // Checking if there's any date in output queue
                    Int32 writeQueueItemsCount = 0;
                    lock (s_outputDataQueueLocker)
                    {
                        writeQueueItemsCount = s_outputDataQueue.Count;
                    }
                    if (writeQueueItemsCount <= 0)
                    {
                        // Suspend the thread until there's no data to write to the output file
                        s_signalOutputDataReady.WaitOne();
                        lock (s_outputDataReadySignalLocker)
                        {
                            s_signalOutputDataReady.Reset();
                        }
                    }

                    // Checking if there's the block of output data with the same sequence number as the write sequence number
                    Boolean isContainsWriteSequenceNumber = false;
                    lock (s_outputDataQueueLocker)
                    {
                        isContainsWriteSequenceNumber = s_outputDataQueue.ContainsKey(s_writeSequenceNumber);
                    }
                    if (!isContainsWriteSequenceNumber)
                    {   
                        // If there's no block with correct write sequence number the the queue, wait for the next block and go round the loop
                        s_signalOutputDataReady.WaitOne();
                        lock (s_outputDataReadySignalLocker)
                        {
                            s_signalOutputDataReady.Reset();
                        }
                    }
                    else
                    {
                        // If thre is a block with correct write sequence number, write it to the output file
                        // Free up compression thread resources to allow file reader function to read a new chunk of data without waiting for compressed block to be written out
                        byte[] buffer;
                        lock (s_outputDataQueueLocker)
                        {
                            buffer = s_outputDataQueue[s_writeSequenceNumber];
                            s_outputDataQueue.Remove(s_writeSequenceNumber);

                            if (s_outputDataQueue.Count > s_maxWriteQueueLength)
                            {
                                s_signalOutputDataQueueReady.Reset();
                            }
                            else
                            {
                                s_signalOutputDataQueueReady.Set();
                            }
                        }

                        s_outputStream.Write(buffer, 0, buffer.Length);

                        lock (s_writeSequenceNumberLocker)
                        {
                            s_writeSequenceNumber++;
                        }
                    }

                    if (s_isDataProcessingDone &&
                        s_outputDataQueue.Count <= 0)
                    {
                        s_isOutputFileWritten = true;
                        return;
                    }
                }
            }
        }

        // Threaded compression function
        private static void BlockCompressionThread(object parameter)
        {
            Int64 threadSequenceNumber = (Int64)parameter;
            byte[] buffer;

            lock (s_inputDataQueueLocker)
            {
                buffer = s_inputDataQueue[threadSequenceNumber];
                s_inputDataQueue.Remove(threadSequenceNumber);
            }

            // TODO: fix potential System.OutOfMemory exception
            using (MemoryStream outputStream = new MemoryStream(buffer.Length))
            {
                using (GZipStream compressionStream = new GZipStream(outputStream, CompressionMode.Compress))
                {
                    compressionStream.Write(buffer, 0, buffer.Length);
                }

                lock (s_outputDataQueueLocker)
                {
                    s_outputDataQueue.Add(threadSequenceNumber, outputStream.ToArray());
                }
            }

            lock (s_outputDataReadySignalLocker)
            {
                s_signalOutputDataReady.Set();
            }

            lock (s_WorkerTreadExitedSignalLocker)
            {
                s_signalWorkerThreadExited.Set();
            }
        }

        // Threaded decompression function
        private static void BlockDeCompressionThread(object parameter)
        {
            Int64 threadSequenceNumber = (Int64)parameter;
            byte[] buffer;

            lock (s_inputDataQueueLocker)
            {
                buffer = s_inputDataQueue[threadSequenceNumber];
                s_inputDataQueue.Remove(threadSequenceNumber);
            }

            // TODO: fix potential System.OutOfMemory exception
            using (MemoryStream outputStream = new MemoryStream(buffer.Length))
            {
                using (GZipStream compressionStream = new GZipStream(outputStream, CompressionMode.Decompress))
                {
                    compressionStream.Write(buffer, 0, buffer.Length);
                }

                lock (s_outputDataQueueLocker)
                {
                    s_outputDataQueue.Add(threadSequenceNumber, outputStream.ToArray());
                }
            }

            lock (s_outputDataReadySignalLocker)
            {
                s_signalOutputDataReady.Set();
            }

            lock (s_WorkerTreadExitedSignalLocker)
            {
                s_signalWorkerThreadExited.Set();
            }
        }

        // Threaded threads dispatcher that starts worker threads and limits their number
        private static void WorkerThreadsDispatcher(object parameter)
        {
            while (!s_isInputFileRead ||
                   s_inputDataQueue.Count > 0 ||
                   s_workerThreads.Count > 0)
            {
                // Sleeping a little to not overload CPU
                if (!s_isInputFileRead)
                {
                    Int32 inputDataQueueLenght;
                    lock (s_inputDataQueueLocker)
                    {
                        inputDataQueueLenght = s_inputDataQueue.Count;
                    }
                    
                    if (inputDataQueueLenght <= 0)
                    {
                        // Wait for a new block of data from the input file
                        s_signalInputDataReady.WaitOne();
                    }
                }

                if (s_workerThreads.Count < s_maxThreadsCount)
                {
                    Thread workerThread = new Thread(BlockCompressionThread);

                    // If there's less than maximum allowed threads are running, spawn a new one                    
                    lock (s_inputDataQueueLocker)
                    {
                        foreach (Int64 threadSequenceNumber in s_inputDataQueue.Keys)
                        {
                            if (!s_workerThreads.ContainsKey(threadSequenceNumber))
                            {
                                workerThread.Name = String.Format("Data processing (seq: {0})", threadSequenceNumber);
                                s_workerThreads.Add(threadSequenceNumber, workerThread);
                                s_workerThreads[threadSequenceNumber].Start(threadSequenceNumber);
                                break; // control amount of threads to fit in CPU cores number
                            }
                        }
                    }
                }

                // Cleaning up used threads
                List<Int64> threadsToRemove = new List<Int64>();
                foreach (Int64 threadSequenceNumber in s_workerThreads.Keys)
                {
                    if (s_workerThreads[threadSequenceNumber].ThreadState == ThreadState.Stopped ||
                        s_workerThreads[threadSequenceNumber].ThreadState == ThreadState.Aborted)
                    {
                        threadsToRemove.Add(threadSequenceNumber);
                    }
                }
                foreach (Int64 threadSequenceNumber in threadsToRemove)
                {
                    s_workerThreads.Remove(threadSequenceNumber);
                }

                if (s_workerThreads.Count >= s_maxThreadsCount)
                {
                    s_signalWorkerThreadExited.WaitOne();
                    lock (s_WorkerTreadExitedSignalLocker)
                    {
                        s_signalWorkerThreadExited.Reset();
                    }
                }
            }
            s_isDataProcessingDone = true;
        }

        static int Main(string[] args)
        {
            // Init local variables
            s_maxThreadsCount = Environment.ProcessorCount;
            s_maxWriteQueueLength = s_maxThreadsCount * 2;
            s_workerThreads = new Dictionary<Int64, Thread>();
            s_inputDataQueue = new Dictionary<Int64, Byte[]>();
            s_outputDataQueue = new Dictionary<Int64, Byte[]>();
            s_readSequenceNumber = 0;
            s_writeSequenceNumber = 0;


            // Starting file reader thread
            //Thread inputFileReadThread = new Thread(FileReadThread);
            Thread inputFileReadThread = new Thread(CompressedFileReadThread);
            inputFileReadThread.Name = "Read input file";
            //SFileReadThreadArguments sFileReadArgs;
            //sFileReadArgs.fileName = s_srcFileName;
            //sFileReadArgs.compressionMode = CompressionMode.Compress;
            //sFileReadArgs.compressionMode = CompressionMode.Decompress;
            //inputFileReadThread.Start(sFileReadArgs);
            inputFileReadThread.Start(s_srcFileName);


            // Starting file writer thread
            FileInfo srcFileInfo = new FileInfo(s_srcFileName);
            //String dstFileName = srcFileInfo.FullName + ".gz";
            String dstFileName = srcFileInfo.FullName.Replace(".gz", null).Replace(".mp4", " (1).mp4");
            Thread outputFileWriteThread = new Thread(FileWriteThread);
            outputFileWriteThread.Name = "Write output file";
            outputFileWriteThread.Start(dstFileName);


            // Starting compression threads manager thread
            Thread workerThreadsManagerThread = new Thread(WorkerThreadsDispatcher);
            workerThreadsManagerThread.Name = "Worker threads manager";
            //workerThreadsManagerThread.Start(null);

            Console.ReadLine();

            return 0;
        }
    }
}
