using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;
using System.Threading;


// TODO: Implement debug files for every of every step of compression and decompression
namespace GZipTest
{
    public static class CGZipCompressor
    {
        #region HARDCODE
        // HARDCODE: size of a buffer per thread
        // WARNING: block decompression thread assumes that this is the maximum possible size of the output buffer
        private static readonly Int32 s_chunkSize = 256 * 1024 * 1024;
        #endregion

        #region FIELDS
        #region Threads
        // Threads pool
        private static Dictionary<Int64, Thread> s_workerThreads;
        // Threads pool locker
        private static readonly Object s_workerThreadsLocker = new Object();

        // Maximum worker threads count
        private static Int32 s_maxThreadsCount;
        #endregion

        #region Block sequences management
        // Read sequence number
        private static Int64 s_readSequenceNumber;

        // Read sequence number
        private static Int64 s_writeSequenceNumber;
        // Write sequence number lock object
        private static readonly Object s_writeSequenceNumberLocker = new Object();
        #endregion

        #region Read/write queues
        // Input data queue
        // Stores blocks that has been read from the input file until they are picked up by Worker threads
        private static Dictionary<Int64, Byte[]> s_readQueue;
        // Read queue lock object
        private static readonly Object s_readQueueLocker = new Object();

        // Output data queue
        // Stores processed (compressed/decompressed) blocks which are produced by Worker threads until they are picked up by output file write thread
        private static Dictionary<Int64, Byte[]> s_writeQueue;
        // Read buffer lock object
        private static readonly Object s_writeQueueLocker = new Object();

        // Maximum lenght of write queue. 
        // After reaching this value file read thread will wait until data from write queue will be written to output file
        private static Int32 s_maxWriteQueueLength;

        // Maximum lenght of read queue.
        // After reaching this value file read thread will wait until data from read queue will be processed by worker thread(s)
        private static Int32 s_maxReadQueueLength;
        #endregion

        #region Operations mode
        // Operations mode (compression or decompression)
        private static CompressionMode s_compressionMode;
        public static CompressionMode CompressionMode
        {
            get
            {
                return s_compressionMode;
            }
            set
            {
                s_compressionMode = value;
            }
        }
        #endregion

        // First three bytes of a GZip file (compressed block) signature
        private static readonly Byte[] s_gZipFileSignature = new Byte[] { 0x1F, 0x8B, 0x08 };

        #region Flags
        // Flag: the input file has been read and the file read thread has exited
        private static Boolean s_isInputFileRead;

        // Flag: all input data has been processed and all worker threads are terminated
        private static Boolean s_isDataProcessingDone;

        // Flag: the output file has been written and file write thread has exited
        private static Boolean s_isOutputFileWritten;
        #endregion

        #region Inter-thread communication signals (wait handles)
        // Event: output data queue is ready to receive new block of processed data
        // Is used to throttle input file read when output file write queue becames to loong
        private static ManualResetEvent s_signalOutputDataQueueReady = new ManualResetEvent(false);

        // Event: a data processing thread finished processing its block of data
        private static ManualResetEvent s_signalWorkerThreadReady = new ManualResetEvent(false);
        // Locker object for s_signalWorkerThreadReady
        private static readonly Object s_workerThreadReadySignalLocker = new Object();

        // Event: a new block of data has been read by input file read thread
        //private static EventWaitHandle s_signalFileReadThreadReady = new EventWaitHandle(false, EventResetMode.AutoReset);
        
        // Event: a worker thread has been terminated
        private static ManualResetEvent s_signalWorkerThreadExited = new ManualResetEvent(false);
        // Locker object for s_signalWorkerThreadExited
        private static readonly Object s_WorkerTreadExitedSignalLocker = new Object();
        #endregion
        #endregion

        #region THREADS DEFINITION
        // File to compress read function (threaded)
        private static void FileReadUncompressedThread(object parameter)
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

                    // TODO: Throttling read in order to not to drain free memory
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

                    lock (s_readQueueLocker)
                    {
                        s_readQueue.Add(s_readSequenceNumber, buffer);
                    }

                    // debug
                    /*
                    using (FileStream partFile = new FileStream(@"d:\tmp\input_part"+s_readSequenceNumber+".bin", FileMode.Create))
                    {
                        partFile.Write(buffer, 0, buffer.Length);
                    }
                    */

                    s_readSequenceNumber++;
                    //s_signalFileReadThreadReady.Set();
                }

                s_isInputFileRead = true;
                //s_signalFileReadThreadReady.Set(); // Unfreeze all awaiting threads
            }
        }

        // File to decompress read function (threaded)
        private static void FileReadCompressedThread(object parameter)
        {
            String fileName = (String)parameter;
            s_isInputFileRead = false;

            Int64 bytesRead;
            Int64 fileReadBufferSize;

            using (FileStream inputStream = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            {

                Byte[] segmentBuffer = null;
                Int32 len;

                while (inputStream.Position < inputStream.Length)
                {
                    // Throtling read in order to allow the file writer thread to write out some data
                    s_signalOutputDataQueueReady.WaitOne();

                    // TODO: Throttling read in order to not to drain free memory
                    if ((inputStream.Length - inputStream.Position) < s_chunkSize)
                    {
                        fileReadBufferSize = (Int32)(inputStream.Length - inputStream.Position);
                    }
                    else
                    {
                        fileReadBufferSize = s_chunkSize;
                    }

                    if (fileReadBufferSize <= 0)
                    {
                        // Ooops
                    }

                    Byte[] fileReadBuffer = new Byte[fileReadBufferSize];
                    bytesRead = inputStream.Read(fileReadBuffer, 0, fileReadBuffer.Length);
                    if (bytesRead <= 0)
                    {
                        // Ooops // Reached the end of the file (not required)
                    }

                    int segmentStartOffset = -1;
                    for (int i = 0; i < fileReadBuffer.Length - 2; i++)
                    {
                        if (fileReadBuffer[i] == s_gZipFileSignature[0] &&
                            fileReadBuffer[i + 1] == s_gZipFileSignature[1] &&
                            fileReadBuffer[i + 2] == s_gZipFileSignature[2])
                        {
                            if (segmentStartOffset < 0)
                            {
                                segmentStartOffset = i;

                                if (segmentBuffer != null)
                                {   // adding to existing file buffer

                                    // Saving current data in file buffer
                                    Byte[] tmpBuffer = segmentBuffer;
                                    len = tmpBuffer.Length + i;

                                    // reallocating file buffer
                                    segmentBuffer = new Byte[len];

                                    Array.Copy(tmpBuffer, 0, segmentBuffer, 0, tmpBuffer.Length);
                                    Array.Copy(fileReadBuffer, 0, segmentBuffer, tmpBuffer.Length, i);
                                    lock (s_readQueueLocker)
                                    {
                                        s_readQueue.Add(s_readSequenceNumber, segmentBuffer);
                                    }

                                    // debug
                                    /*
                                    using (FileStream partFile = new FileStream(@"d:\tmp\detected_compressed_part" + s_readSequenceNumber + ".gz", FileMode.Create))
                                    {
                                        partFile.Write(segmentBuffer, 0, segmentBuffer.Length);
                                    }
                                    */

                                    s_readSequenceNumber++;
                                    segmentStartOffset = i;
                                    segmentBuffer = null;
                                    //s_signalFileReadThreadReady.Set();
                                }
                            }
                            else
                            {
                                len = i - segmentStartOffset;
                                segmentBuffer = new Byte[len];

                                Array.Copy(fileReadBuffer, segmentStartOffset, segmentBuffer, 0, len);
                                lock (s_readQueueLocker)
                                {
                                    s_readQueue.Add(s_readSequenceNumber, segmentBuffer);
                                }

                                // debug
                                /*
                                using (FileStream partFile = new FileStream(@"d:\tmp\detected_compressed_part" + s_readSequenceNumber + ".gz", FileMode.Create))
                                {
                                    partFile.Write(segmentBuffer, 0, segmentBuffer.Length);
                                }
                                */

                                s_readSequenceNumber++;
                                segmentStartOffset = i;
                                segmentBuffer = null;
                                //s_signalFileReadThreadReady.Set();
                            }
                        }

                        if (i == fileReadBuffer.Length - s_gZipFileSignature.Length &&
                            segmentStartOffset < fileReadBuffer.Length)
                        {
                            if (segmentStartOffset < 0)
                            {
                                segmentStartOffset = 0;
                            }

                            if (segmentBuffer != null)
                            {
                                // Saving current data in file buffer
                                Byte[] tmpBuffer = segmentBuffer;
                                len = tmpBuffer.Length + i + s_gZipFileSignature.Length; // compensating

                                // reallocating file buffer
                                segmentBuffer = new Byte[len];

                                Array.Copy(tmpBuffer, 0, segmentBuffer, 0, tmpBuffer.Length);
                                Array.Copy(fileReadBuffer, 0, segmentBuffer, tmpBuffer.Length, i + s_gZipFileSignature.Length);
                            }
                            else
                            {
                                len = fileReadBuffer.Length - segmentStartOffset;
                                segmentBuffer = new Byte[len];

                                Array.Copy(fileReadBuffer, segmentStartOffset, segmentBuffer, 0, len);
                            }

                            if (inputStream.Position >= inputStream.Length)
                            {
                                lock (s_readQueueLocker)
                                {
                                    s_readQueue.Add(s_readSequenceNumber, segmentBuffer);
                                }

                                // debug
                                /*
                                using (FileStream partFile = new FileStream(@"d:\tmp\detected_compressed_part" + s_readSequenceNumber + ".gz", FileMode.Create))
                                {
                                    partFile.Write(segmentBuffer, 0, segmentBuffer.Length);
                                }
                                */

                                s_readSequenceNumber++;
                                segmentStartOffset = -1;  // doesn't matter, but still
                                segmentBuffer = null; // doesn't matter, but still
                                //s_signalFileReadThreadReady.Set();
                            }
                        }
                    }
                }
            }

            s_isInputFileRead = true;
            //s_signalFileReadThreadReady.Set(); // Unfreeze all awaiting threads
        }

        // Universal (compressed and decompressed) file write function (threaded)
        private static void FileWriteThread(object parameter)
        {
            String fileName = (String)parameter;

            s_isOutputFileWritten = false;

            using (FileStream outputStream = new FileStream(fileName, FileMode.Create))
            {
                // Initial command to file read thread to start reading
                s_signalOutputDataQueueReady.Set();

                while (!s_isOutputFileWritten) // Don't required, but it's a good fail-safe measure
                {
                    // Checking if there's any date in output queue
                    Int32 writeQueueItemsCount = 0;
                    lock (s_writeQueueLocker)
                    {
                        writeQueueItemsCount = s_writeQueue.Count;
                    }

                    // Suspend the thread until there's no data to write to the output file
                    if (writeQueueItemsCount <= 0)
                    {
                        if (s_isDataProcessingDone)
                        {
                            s_isOutputFileWritten = true;
                            break;
                        }                        
                    }

                    // Checking if there's a block of output data with the same sequence number as the write sequence number
                    Boolean isContainsWriteSequenceNumber = false;
                    lock (s_writeQueueLocker)
                    {
                        isContainsWriteSequenceNumber = s_writeQueue.ContainsKey(s_writeSequenceNumber);
                    }
                    if (!isContainsWriteSequenceNumber)
                    {
                        // If there's no block with correct write sequence number the the queue, wait for the next block and go round the loop
                        //s_signalWorkerThreadReady.WaitOne();
                        //lock (s_workerThreadReadySignalLocker)
                        //{
                        //    s_signalWorkerThreadReady.Reset();
                        //}
                    }
                    else
                    {
                        // If there is a block with correct write sequence number, write it to the output file
                        byte[] buffer;
                        lock (s_writeQueueLocker)
                        {
                            buffer = s_writeQueue[s_writeSequenceNumber];
                            s_writeQueue.Remove(s_writeSequenceNumber);

                            if (s_writeQueue.Count > s_maxWriteQueueLength)
                            {
                                s_signalOutputDataQueueReady.Reset();
                            }
                            else
                            {
                                s_signalOutputDataQueueReady.Set();
                            }
                        }

                        outputStream.Write(buffer, 0, buffer.Length);

                        // debug
                        /*
                        switch (s_compressionMode)
                        {
                            case CompressionMode.Compress:
                                using (FileStream partFile = new FileStream(@"e:\tmp\compressed_part" + s_writeSequenceNumber + ".gz", FileMode.Create))
                                {
                                    partFile.Write(buffer, 0, buffer.Length);
                                }
                                break;

                            case CompressionMode.Decompress:
                                using (FileStream partFile = new FileStream(@"e:\tmp\decompressed_part" + s_writeSequenceNumber + ".bin", FileMode.Create))
                                {
                                    partFile.Write(buffer, 0, buffer.Length);
                                }
                                break;

                            default:
                                break;
                        }*/

                        lock (s_writeSequenceNumberLocker)
                        {
                            s_writeSequenceNumber++;
                        }
                    }

                    Thread.Sleep(1000); // TODO: replace this workaround with signals
                }
            }
        }

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

            using (MemoryStream outputStream = new MemoryStream(buffer.Length))
            {
                using (GZipStream compressionStream = new GZipStream(outputStream, CompressionMode.Compress))
                {
                    compressionStream.Write(buffer, 0, buffer.Length);
                }

                lock (s_writeQueueLocker)
                {
                    s_writeQueue.Add(threadSequenceNumber, outputStream.ToArray());
                }
            }
        }

        // Threaded decompression function
        private static void BlockDecompressionThread(object parameter)
        {
            Int64 threadSequenceNumber = (Int64)parameter;
            byte[] buffer;

            lock (s_readQueueLocker)
            {
                buffer = s_readQueue[threadSequenceNumber];
                s_readQueue.Remove(threadSequenceNumber);
            }

            using (MemoryStream inputStream = new MemoryStream(buffer))
            {
                int bytesRead;
                Byte[] outputBuffer = new Byte[s_chunkSize];

                using (GZipStream gZipStream = new GZipStream(inputStream, CompressionMode.Decompress))
                {
                    bytesRead = gZipStream.Read(outputBuffer, 0, s_chunkSize);
                }

                Byte[] decompressedData = new Byte[bytesRead];
                Array.Copy(outputBuffer, decompressedData, decompressedData.Length);
                outputBuffer = null;

                lock (s_writeQueueLocker)
                {
                    s_writeQueue.Add(threadSequenceNumber, decompressedData);
                }
                decompressedData = null;
            }
        }

        // Threaded threads dispatcher that starts worker threads and limits their number
        private static void WorkerThreadsDispatcher(object parameter)
        {
            // TODO: Implement correct signalling
            // TODO: Lock queue and threads before get Count
            Int32 readQueueCount = 0;
            do
            {
                // Sleeping a little to not overload CPU
                /*
                if (!s_isInputFileRead)
                {
                    Int32 inputDataQueueLenght;
                    lock (s_readQueueLocker)
                    {
                        inputDataQueueLenght = s_readQueue.Count;
                    }

                    if (inputDataQueueLenght <= 0)
                    {
                        // Wait for a new block of data from the input file
                        //s_signalFileReadThreadReady.WaitOne();
                    }
                }*/

                if (s_workerThreads.Count < s_maxThreadsCount)
                {
                    Thread workerThread = null;
                    switch (s_compressionMode)
                    {
                        case CompressionMode.Compress:
                            workerThread = new Thread(BlockCompressionThread);
                            break;

                        case CompressionMode.Decompress:
                            workerThread = new Thread(BlockDecompressionThread);
                            break;

                        default:
                            break;
                    }

                    // If there's less than maximum allowed threads are running, spawn a new one                    
                    lock (s_readQueueLocker)
                    {
                        foreach (Int64 threadSequenceNumber in s_readQueue.Keys)
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
                else
                {
                    List<Int32> finishedThreads = new List<Int32>();
                    foreach (Int32 threadSequenceNumber in s_workerThreads.Keys)
                    {
                        if (s_workerThreads[threadSequenceNumber].ThreadState == ThreadState.Stopped ||
                            s_workerThreads[threadSequenceNumber].ThreadState == ThreadState.Aborted)
                        {
                            finishedThreads.Add(threadSequenceNumber);
                        }
                    }

                    foreach (Int32 threadSequenceNumber in finishedThreads)
                    {
                        s_workerThreads.Remove(threadSequenceNumber);
                    }
                }

                Thread.Sleep(1000);

                lock (s_readQueueLocker)
                {
                    readQueueCount = s_readQueue.Count;
                }

            } while (!s_isInputFileRead ||
                     readQueueCount > 0);

            s_isDataProcessingDone = true;
        }
        #endregion

        #region FUNCTIONS / PROCEDURES
        // Initialize internal variables
        private static void Initialize()
        {
            // Init local variables
            s_workerThreads = new Dictionary<Int64, Thread>();
            s_readQueue = new Dictionary<Int64, Byte[]>();
            s_writeQueue = new Dictionary<Int64, Byte[]>();
            s_isInputFileRead = false;
            s_isDataProcessingDone = false;
            s_isOutputFileWritten = false;

            //s_signalOutputDataQueueReady.Reset();
            s_signalWorkerThreadExited.Reset();

            s_readSequenceNumber = 0;
            s_writeSequenceNumber = 0;

            // DEBUG 
            //s_maxThreadsCount = Environment.ProcessorCount;
            s_maxThreadsCount = 1; // DEBUG
            s_maxWriteQueueLength = s_maxThreadsCount * 2; // HARDCODE!
            s_maxReadQueueLength = s_maxThreadsCount * 2; // HARDCODE!
        }

        // Execute main compression / decompression logic
        public static void Run(String inputFilePath, String outputFilePath)
        {
            Initialize();

            // Starting compression threads manager thread
            Thread workerThreadsManagerThread = new Thread(WorkerThreadsDispatcher);
            workerThreadsManagerThread.Name = "Worker threads manager";
            workerThreadsManagerThread.Start(null);

            // Initializing input file read thread
            Thread inputFileReadThread = null;
            switch (s_compressionMode)
            {
                case CompressionMode.Compress:
                    // Starting file reader thread
                    inputFileReadThread = new Thread(FileReadUncompressedThread);
                    inputFileReadThread.Name = "Read uncompressed input file";
                    break;

                case CompressionMode.Decompress:
                    // Starting file reader thread
                    inputFileReadThread = new Thread(FileReadCompressedThread);
                    inputFileReadThread.Name = "Read compressed input file";
                    break;

                default:
                    break;
            }

            // Starting previously initialized input file read thread
            inputFileReadThread.Start(inputFilePath);

            // Starting compressed file write thread
            Thread outputFileWriteThread = new Thread(FileWriteThread);
            outputFileWriteThread.Name = "Write output file";
            outputFileWriteThread.Start(outputFilePath);

            inputFileReadThread.Join();
            outputFileWriteThread.Join();
            workerThreadsManagerThread.Join();
        }

        // Execute main compression / decompression logic
        // Overload with compression mode specification
        public static void Run(String sourceFilePath, String destinationFilePath, CompressionMode mode)
        {
            s_compressionMode = mode;
            Run(sourceFilePath, destinationFilePath);
        }
        #endregion
    }

    class Program
    {      
        static int Main(string[] args)
        {
            String inputFilePath = @"c:\tmp\Iteration4-2x4CPU_16GB_RAM.blg";
            //String inputFilePath = @"c:\tmp\uncompressed-files-archive.zip";
            //String inputFilePath = @"c:\tmp\";
            //String inputFilePath = @"e:\Downloads\Movies\Imaginaerum.2012.1080p.BluRay.x264.YIFY.mp4";
            //String inputFilePath = @"e:\Downloads\Movies\Mad.Max.Fury.Road.2015.1080p.BluRay.AC3.x264-ETRG.mkv";

            FileInfo inputFileInfo = new FileInfo(inputFilePath);
            String compressedFilePath = inputFileInfo.FullName + ".gz";
            String decompressedFilePath = inputFileInfo.Directory + @"\" + inputFileInfo.Name.Replace(inputFileInfo.Extension, null) + " (1)" + inputFileInfo.Extension;

            CGZipCompressor.Run(inputFilePath, compressedFilePath, CompressionMode.Compress);
            CGZipCompressor.Run(compressedFilePath, decompressedFilePath, CompressionMode.Decompress);

            Console.ReadLine();

            return 0;
        }
    }
}
