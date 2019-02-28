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
        // HARDCODE: size of a buffer per thread
        // WARNING: block decompression thread assumes that this is the maximum possible size of the output buffer
        private static readonly Int32 s_chunkSize = 256 * 1024 * 1024;

        // Threads pool
        private static Dictionary<Int64, Thread> s_workerThreads;

        // Read sequence number
        private static Int64 s_readSequenceNumber;

        // Read sequence number
        private static Int64 s_writeSequenceNumber;
        // Write sequence number lock object
        private static readonly Object s_writeSequenceNumberLocker = new Object();

        // Maximum worker threads count
        private static Int32 s_maxThreadsCount;

        // Maximum lenght of write queue. 
        // After reaching this value file read thread will wait until write queue will be sorten below this value
        private static Int32 s_maxWriteQueueLength;

        // Output file stream to write data to
        private static FileStream s_outputStream;

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

        // QUEUES
        // Input data queue
        // Stores blocks that has been read from the input file until they are picked up by Worker threads
        private static Dictionary<Int64, Byte[]> s_inputDataQueue;
        // Write buffer lock object
        private static readonly Object s_inputDataQueueLocker = new Object();

        // Output data queue
        // Stores processed (compressed/decompressed) blocks which are produced by Worker threads until they are picked up by output file write thread
        private static Dictionary<Int64, Byte[]> s_outputDataQueue;
        // Read buffer lock object
        private static readonly Object s_outputDataQueueLocker = new Object();


        // FLAGS
        // Flag: the input file has been read and the file read thread has exited
        private static Boolean s_isInputFileRead;

        // Flag: all input data has been processed and all worker threads are terminated
        private static Boolean s_isDataProcessingDone;

        // Flag: the output file has been written and file write thread has exited
        private static Boolean s_isOutputFileWritten;

       
        // INTER-THREAD COMMUNICATION SIGNALS
        // Event: output data queue is ready to receive new block of processed data
        // Is used to throttle input file read when output file write queue becames to loong
        private static ManualResetEvent s_signalOutputDataQueueReady = new ManualResetEvent(false);

        // Event: a data processing thread finished processing its block of data
        private static ManualResetEvent s_signalOutputDataReady = new ManualResetEvent(false);
        // Locker object for s_outputDataReadySignal
        private static readonly Object s_outputDataReadySignalLocker = new Object();

        // Event: a new block of data has been read by input file read thread
        private static EventWaitHandle s_signalInputDataReady = new EventWaitHandle(false, EventResetMode.AutoReset);
        
        // Event: a worker thread has been terminated
        private static ManualResetEvent s_signalWorkerThreadExited = new ManualResetEvent(false);
        // Locker object for s_signalWorkerThreadExited
        private static readonly Object s_WorkerTreadExitedSignalLocker = new Object();


        // FUNCTIONS / PROCEDURES
        // File to compress read function (threaded)
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

                    // debug
                    /*
                    using (FileStream partFile = new FileStream(@"e:\tmp\input_part"+s_readSequenceNumber+".bin", FileMode.Create))
                    {
                        partFile.Write(buffer, 0, buffer.Length);
                    }
                    */

                    s_readSequenceNumber++;
                    s_signalInputDataReady.Set();
                }

                s_isInputFileRead = true;
                s_signalInputDataReady.Set(); // Unfreeze all awaiting threads
            }
        }

        // File to decompress read function (threaded)
        private static void CompressedFileReadThread(object parameter)
        {
            String fileName = (String)parameter;
            s_isInputFileRead = false;

            Byte[] gzipFileSignature = new Byte[] { 0x1F, 0x8B, 0x08 };

            Int64 bytesRead;
            Int64 fileReadBufferSize;

            using (FileStream inputStream = new FileStream(fileName, FileMode.Open, FileAccess.Read))
            {

                Byte[] segmentBuffer = null;
                Int32 len;

                while (inputStream.Position < inputStream.Length)
                {
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
                        if (fileReadBuffer[i] == gzipFileSignature[0] &&
                            fileReadBuffer[i + 1] == gzipFileSignature[1] &&
                            fileReadBuffer[i + 2] == gzipFileSignature[2])
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
                                    s_inputDataQueue.Add(s_readSequenceNumber, segmentBuffer);

                                    // debug
                                    /*
                                    using (FileStream partFile = new FileStream(@"e:\tmp\detected_compressed_part" + s_readSequenceNumber + ".gz", FileMode.Create))
                                    {
                                        partFile.Write(segmentBuffer, 0, segmentBuffer.Length);
                                    }
                                    */

                                    s_readSequenceNumber++;
                                    segmentStartOffset = i;
                                    segmentBuffer = null;
                                    s_signalInputDataReady.Set();
                                }
                            }
                            else
                            {
                                len = i - segmentStartOffset;
                                segmentBuffer = new Byte[len];

                                Array.Copy(fileReadBuffer, segmentStartOffset, segmentBuffer, 0, len);
                                s_inputDataQueue.Add(s_readSequenceNumber, segmentBuffer);

                                // debug
                                /*
                                using (FileStream partFile = new FileStream(@"e:\tmp\detected_compressed_part" + s_readSequenceNumber + ".gz", FileMode.Create))
                                {
                                    partFile.Write(segmentBuffer, 0, segmentBuffer.Length);
                                }
                                */

                                s_readSequenceNumber++;
                                segmentStartOffset = i;
                                segmentBuffer = null;
                                s_signalInputDataReady.Set();
                            }
                        }

                        if (i == fileReadBuffer.Length - gzipFileSignature.Length &&
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
                                len = tmpBuffer.Length + i + gzipFileSignature.Length; // compensating

                                // reallocating file buffer
                                segmentBuffer = new Byte[len];

                                Array.Copy(tmpBuffer, 0, segmentBuffer, 0, tmpBuffer.Length);
                                Array.Copy(fileReadBuffer, 0, segmentBuffer, tmpBuffer.Length, i + gzipFileSignature.Length);
                            }
                            else
                            {
                                len = fileReadBuffer.Length - segmentStartOffset;
                                segmentBuffer = new Byte[len];

                                Array.Copy(fileReadBuffer, segmentStartOffset, segmentBuffer, 0, len);
                            }

                            if (inputStream.Position >= inputStream.Length)
                            {
                                s_inputDataQueue.Add(s_readSequenceNumber, segmentBuffer);

                                // debug
                                /*
                                using (FileStream partFile = new FileStream(@"e:\tmp\detected_compressed_part" + s_readSequenceNumber + ".gz", FileMode.Create))
                                {
                                    partFile.Write(segmentBuffer, 0, segmentBuffer.Length);
                                }
                                */

                                s_readSequenceNumber++;
                                segmentStartOffset = -1;  // doesn't matter, but still
                                segmentBuffer = null; // doesn't matter, but still
                                s_signalInputDataReady.Set();
                            }
                        }
                    }

                    // TODO: Handle unexpected end of the chunk
                    Thread.Sleep(1000);
                }
            }

            s_isInputFileRead = true;
            s_signalInputDataReady.Set(); // Unfreeze all awaiting threads
        }

        // Universal (compressed and decompressed) file write function (threaded)
        private static void FileWriteThread(object parameter)
        {
            String fileName = (String)parameter;

            s_isOutputFileWritten = false;

            using (s_outputStream = new FileStream(fileName, FileMode.Create))
            {
                // Initial command to file read thread to start reading
                s_signalOutputDataQueueReady.Set();

                while (!s_isOutputFileWritten) // Don't required, but it's a good fail-safe measure
                {
                    // Checking if there's any date in output queue
                    Int32 writeQueueItemsCount = 0;
                    lock (s_outputDataQueueLocker)
                    {
                        writeQueueItemsCount = s_outputDataQueue.Count;
                    }

                    // Suspend the thread until there's no data to write to the output file
                    if (writeQueueItemsCount <= 0)
                    {
                        if (s_isDataProcessingDone)
                        {
                            s_isOutputFileWritten = true;
                            break;
                        }

                        /* Doesn't work as intended
                         * TODO: Fix signaling
                        s_signalOutputDataReady.WaitOne();
                        lock (s_outputDataReadySignalLocker)
                        {
                            s_signalOutputDataReady.Reset();
                        }
                        */
                    }

                    // Checking if there's a block of output data with the same sequence number as the write sequence number
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
                        // If there is a block with correct write sequence number, write it to the output file
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

                        // debug
                        /*
                        using (FileStream partFile = new FileStream(@"e:\tmp\compressed_part" + s_writeSequenceNumber + ".gz", FileMode.Create))
                        {
                            partFile.Write(buffer, 0, buffer.Length);
                        }*/
                        /*
                        using (FileStream partFile = new FileStream(@"e:\tmp\decompressed_part" + s_writeSequenceNumber + ".bin", FileMode.Create))
                        {
                            partFile.Write(buffer, 0, buffer.Length);
                        }
                        */

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

            lock (s_inputDataQueueLocker)
            {
                buffer = s_inputDataQueue[threadSequenceNumber];
                s_inputDataQueue.Remove(threadSequenceNumber);
            }

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

                lock (s_outputDataQueueLocker)
                {
                    s_outputDataQueue.Add(threadSequenceNumber, decompressedData);
                }
                decompressedData = null;
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
            // TODO: Implement correct signalling

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
                    Thread workerThread = null;
                    switch (s_compressionMode)
                    {
                        case CompressionMode.Compress:
                            workerThread = new Thread(BlockCompressionThread);
                            break;

                        case CompressionMode.Decompress:
                            workerThread = new Thread(BlockDeCompressionThread);
                            break;

                        default:
                            break;
                    }

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

                if (s_workerThreads.Count > s_maxThreadsCount)
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

        // Initialize internal variables
        private static void Initialize()
        {
            // Init local variables
            s_workerThreads = new Dictionary<Int64, Thread>();
            s_inputDataQueue = new Dictionary<Int64, Byte[]>();
            s_outputDataQueue = new Dictionary<Int64, Byte[]>();
            s_outputStream = null;
            s_isInputFileRead = false;
            s_isDataProcessingDone = false;
            s_isOutputFileWritten = false;

            s_signalOutputDataQueueReady.Reset();
            s_signalInputDataReady.Reset();
            s_signalInputDataReady.Reset();
            s_signalWorkerThreadExited.Reset();

            s_readSequenceNumber = 0;
            s_writeSequenceNumber = 0;

            // DEBUG 
            s_maxThreadsCount = Environment.ProcessorCount;
            //s_maxThreadsCount = 1; // DEBUG
            s_maxWriteQueueLength = s_maxThreadsCount * 2; // HARDCODE!
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
                    inputFileReadThread = new Thread(FileReadThread);
                    inputFileReadThread.Name = "Read uncompressed input file";
                    break;

                case CompressionMode.Decompress:
                    // Starting file reader thread
                    inputFileReadThread = new Thread(CompressedFileReadThread);
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
    }

    class Program
    {      
        static int Main(string[] args)
        {
            //String inputFilePath = @"c:\tmp\Iteration4-2x4CPU_16GB_RAM.blg";
            String inputFilePath = @"e:\Downloads\Movies\Imaginaerum.2012.1080p.BluRay.x264.YIFY.mp4";
            //String inputFilePath = @"e:\Downloads\Movies\Mad.Max.Fury.Road.2015.1080p.BluRay.AC3.x264-ETRG.mkv";

            FileInfo inputFileInfo = new FileInfo(inputFilePath);
            String compressedFilePath = inputFileInfo.FullName + ".gz";
            String decompressedFilePath = inputFileInfo.Directory + @"\" + inputFileInfo.Name.Replace(inputFileInfo.Extension, null) + " (1)" + inputFileInfo.Extension;

            //CGZipCompressor.Run(inputFilePath, compressedFilePath, CompressionMode.Compress);
            CGZipCompressor.Run(compressedFilePath, decompressedFilePath, CompressionMode.Decompress);

            Console.ReadLine();

            return 0;
        }
    }
}
