﻿using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;


namespace GZipTest
{
    public static class CGZipCompressor
    {
        #region HARDCODED SETTINGS
        // HARDCODE!
        // Data block size
        // When compressing: size of data buffer per each thread
        // When decompressing: size of read buffer for compressed file
        // WARNING: block decompression thread assumes that this is the maximum possible size of the output buffer
        private static readonly Int32 s_chunkSize = 128 * 1024 * 1024;

        // HARDCODE!
        // Maximum lenght of read queue.
        // After reaching this value file read thread will wait until data from read queue will be processed by worker thread(s)
        private static readonly Int32 s_maxReadQueueLength = 4;

        // HARDCODE!
        // Maximum lenght of write queue. 
        // After reaching this value file read thread will wait until data from write queue will be written to output file
        private static readonly Int32 s_maxWriteQueueLength = 4;

        // HARDCODE!
        // Mamimum threads limiter subtraction value (useful when you don't want to hung your PC during (de)compression)
        private static readonly Int32 s_maxThreadsSubtraction = 1;
        #endregion

        #region FIELDS
        // First three bytes of a GZip file (compressed block) signature
        private static readonly Byte[] s_gZipFileSignature = new Byte[] { 0x1F, 0x8B, 0x08 };

        #region Emergency shutdown
        // Emergency shutdown flag
        private static Boolean s_isEmergencyShutdown;
        public static Boolean IsEmergencyShutdown
        {
            get
            {
                return s_isEmergencyShutdown;
            }
        }

        // Emergency shutdown message
        private static String s_emergencyShutdownMessage;
        public static String EmergenceShutdownMessage
        {
            get
            {
                if (String.IsNullOrEmpty(s_emergencyShutdownMessage))
                {
                    s_emergencyShutdownMessage = "";
                    
                }
                return s_emergencyShutdownMessage;
            }
        }
        #endregion

        #region Threads
        #region Worker threads
        // Threads pool
        private static Dictionary<Int64, Thread> s_workerThreads;
        private static Dictionary<Int64, Thread> WorkerThreads
        {
            get
            {
                if (s_workerThreads == null)
                {
                    s_workerThreads = new Dictionary<Int64, Thread>();
                }
                return s_workerThreads;
            }

            set
            {
                if (s_workerThreads == null)
                {
                    s_workerThreads = new Dictionary<Int64, Thread>();
                }
                if (value == null)
                {
                    s_workerThreads.Clear();
                }
                else
                {
                    s_workerThreads = value;
                }
            }
        }

        // Threads pool locker
        private static readonly Object s_workerThreadsLocker = new Object();

        // Maximum worker threads count
        private static Int32 s_maxThreadsCount;
        #endregion

        // Worker threads manager thread
        private static Thread s_workerThreadsManagerThread;

        // Input (un)compressed file read thread
        private static Thread s_inputFileReadThread;

        // Output (de)compressed file write thread
        private static Thread s_outputFileWriteThread;
        #endregion

        #region Block sequences management
        // Read sequence number
        private static Int64 s_readSequenceNumber;

        // Read sequence number
        private static Int64 s_writeSequenceNumber;
        // Write sequence number lock object
        private static readonly Object s_writeSequenceNumberLocker = new Object();
        #endregion

        #region Queues
        #region Read queue
        // Stores blocks that has been read from the input file until they are picked up by Worker threads
        private static Dictionary<Int64, Byte[]> s_readQueue;
        private static Dictionary<Int64, Byte[]> ReadQueue
        {
            get
            {
                if (s_readQueue == null)
                {
                    s_readQueue = new Dictionary<Int64, Byte[]>();
                }
                return s_readQueue;
            }

            set
            {
                if (s_readQueue == null)
                {
                    s_readQueue = new Dictionary<Int64, Byte[]>();
                }
                if (value == null)
                {
                    s_readQueue.Clear();
                }
                else
                {
                    s_readQueue = value;
                }
            }
        }

        // Read queue lock object
        private static readonly Object s_readQueueLocker = new Object();
        #endregion

        #region Write queue
        // Stores processed (compressed/decompressed) blocks which are produced by Worker threads until they are picked up by output file write thread
        private static Dictionary<Int64, Byte[]> s_writeQueue;
        private static Dictionary<Int64, Byte[]> WriteQueue
        {
            get
            {
                if (s_writeQueue == null)
                {
                    s_writeQueue = new Dictionary<Int64, Byte[]>();
                }
                return s_writeQueue;
            }

            set
            {
                if (s_writeQueue == null)
                {
                    s_writeQueue = new Dictionary<Int64, Byte[]>();
                }
                if (value == null)
                {
                    s_writeQueue.Clear();
                }
                else
                {
                    s_writeQueue = value;
                }
            }
        }
        
        // Read buffer lock object
        private static readonly Object s_writeQueueLocker = new Object();
        #endregion
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
        private static readonly ManualResetEvent s_signalWorkerThreadReady = new ManualResetEvent(false);
        // Locker object for s_signalWorkerThreadReady
        private static readonly Object s_workerThreadReadySignalLocker = new Object();

        // Event: a worker thread has been terminated
        private static readonly EventWaitHandle s_signalWorkerThreadExited = new EventWaitHandle(false, EventResetMode.AutoReset);
        // Locker object for s_signalWorkerThreadExited
        private static readonly Object s_WorkerTreadExitedSignalLocker = new Object();

        // Event: a block of output data has been written to the output file
        private static EventWaitHandle s_signalOutputDataWritten = new EventWaitHandle(false, EventResetMode.AutoReset);
        #endregion
        #endregion

        #region THREADS DEFINITION
        // File to compress read function (threaded)
        private static void FileReadUncompressedThread(object parameter)
        {
            s_isInputFileRead = false;

            if (s_isEmergencyShutdown)
            {
                return;
            }

            try
            {
                if (parameter == null)
                {
                    throw new ArgumentNullException("parameter", "Input uncompressed file path for the File Read thread is null");
                }
                String fileName = (String)parameter;    

                using (FileStream inputStream = new FileStream(fileName, FileMode.Open, FileAccess.Read))
                {
                    Int64 bytesRead;
                    Int64 bufferSize;

                    while (inputStream.Position < inputStream.Length)
                    {
                        // Throtling read of thre file until the file writer thread signals that output data queue is ready to receive more data
                        s_signalOutputDataQueueReady.WaitOne();

                        // Throttling read of the input file in order to not to drain free memory
                        // If the read queue lenght is greather than maximum allowed value
                        Int32 readQueueItemsCount;
                        lock (s_readQueueLocker)
                        {
                            readQueueItemsCount = ReadQueue.Count;
                        }
                        if (readQueueItemsCount >= s_maxReadQueueLength)
                        {
                            // Until a block of data has been written to the output file
                            s_signalOutputDataWritten.WaitOne();

                            // And re-evaluate this contidition
                            continue;
                        }

                        // Calculating read block size
                        if ((inputStream.Length - inputStream.Position) < s_chunkSize)
                        {
                            bufferSize = (Int32)(inputStream.Length - inputStream.Position);
                        }
                        else
                        {
                            bufferSize = s_chunkSize;
                        }

                        // Is block size correct?
                        if (bufferSize <= 0)
                        {
                            throw new IndexOutOfRangeException("Current position in input stream is beyond the end of the file");
                        }

                        // Allocating read buffer and reading a block from the file
                        Byte[] buffer = new Byte[bufferSize];
                        bytesRead = inputStream.Read(buffer, 0, buffer.Length);
                        if (bytesRead <= 0)
                        {
                            throw new InvalidDataException("An attemp to read from input stream file has returned no data");
                        }

                        lock (s_readQueueLocker)
                        {
                            ReadQueue.Add(s_readSequenceNumber, buffer);
                        }

                        s_readSequenceNumber++;
                    }
                }
            }
            catch (ThreadAbortException)
            {
                // No need to spoil probably existing emergency shutdown message
                s_isEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in Uncompressed File Read thread caused the process to stop: {0}", ex.Message);
            }

            s_isInputFileRead = true;
        }

        // File to decompress read function (threaded)
        private static void FileReadCompressedThread(object parameter)
        {
            s_isInputFileRead = false;

            if (s_isEmergencyShutdown)
            {
                return;
            }

            try
            {
                if (parameter == null)
                {
                    throw new ArgumentNullException("parameter", "Input compressed file path for File Read thread is null");
                }
                String fileName = (String)parameter;

                Int64 bytesRead;
                Int64 bufferSize;

                using (FileStream inputStream = new FileStream(fileName, FileMode.Open, FileAccess.Read))
                {

                    Byte[] segmentBuffer = null;

                    while (inputStream.Position < inputStream.Length)
                    {
                        // Throtling read of thre file until the file writer thread signals that output data queue is ready to receive more data
                        s_signalOutputDataQueueReady.WaitOne();

                        // Throttling read of the input file in order to not to drain free memory
                        // If the read queue lenght is greather than maximum allowed value
                        Int32 readQueueItemsCount;
                        lock (s_readQueueLocker)
                        {
                            readQueueItemsCount = ReadQueue.Count;
                        }
                        if (readQueueItemsCount >= s_maxReadQueueLength)
                        {
                            // Until a block of data has been written to the output file
                            s_signalOutputDataWritten.WaitOne();

                            // And re-evaluate this contidition
                            continue;
                        }

                        // Calculating read block size
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
                            throw new IndexOutOfRangeException("Current position in input stream is beyond the end of the file");
                        }

                        Byte[] buffer = new Byte[bufferSize];
                        bytesRead = inputStream.Read(buffer, 0, buffer.Length);
                        if (bytesRead <= 0)
                        {
                            throw new InvalidDataException("An attemp to read from input stream file has returned no data");
                        }

                        int segmentStartOffset = -1;
                        for (int i = 0; i < buffer.Length - 2; i++)
                        {
                            if (buffer[i] == s_gZipFileSignature[0] &&
                                buffer[i + 1] == s_gZipFileSignature[1] &&
                                buffer[i + 2] == s_gZipFileSignature[2])
                            {
                                if (segmentStartOffset < 0)
                                {
                                    segmentStartOffset = i;

                                    if (segmentBuffer != null)
                                    {   // adding to existing file buffer

                                        // Saving current data in file buffer
                                        Byte[] tmpBuffer = segmentBuffer;
                                        bufferSize = tmpBuffer.Length + i;

                                        // reallocating file buffer
                                        segmentBuffer = new Byte[bufferSize];

                                        Array.Copy(tmpBuffer, 0, segmentBuffer, 0, tmpBuffer.Length);
                                        Array.Copy(buffer, 0, segmentBuffer, tmpBuffer.Length, i);
                                        lock (s_readQueueLocker)
                                        {
                                            ReadQueue.Add(s_readSequenceNumber, segmentBuffer);
                                        }

                                        #region Debug
                                        /*
                                        using (FileStream partFile = new FileStream(@"d:\tmp\detected_compressed_part" + s_readSequenceNumber + ".gz", FileMode.Create))
                                        {
                                            partFile.Write(segmentBuffer, 0, segmentBuffer.Length);
                                        }
                                        */
                                        #endregion

                                        s_readSequenceNumber++;
                                        segmentStartOffset = i;
                                        segmentBuffer = null;
                                    }
                                }
                                else
                                {
                                    bufferSize = i - segmentStartOffset;
                                    segmentBuffer = new Byte[bufferSize];

                                    Array.Copy(buffer, segmentStartOffset, segmentBuffer, 0, bufferSize);
                                    lock (s_readQueueLocker)
                                    {
                                        ReadQueue.Add(s_readSequenceNumber, segmentBuffer);
                                    }

                                    #region Debug
                                    /*
                                    using (FileStream partFile = new FileStream(@"d:\tmp\detected_compressed_part" + s_readSequenceNumber + ".gz", FileMode.Create))
                                    {
                                        partFile.Write(segmentBuffer, 0, segmentBuffer.Length);
                                    }
                                    */
                                    #endregion

                                    s_readSequenceNumber++;
                                    segmentStartOffset = i;
                                    segmentBuffer = null;
                                }
                            }

                            if (i == buffer.Length - s_gZipFileSignature.Length &&
                                segmentStartOffset < buffer.Length)
                            {
                                if (segmentStartOffset < 0)
                                {
                                    segmentStartOffset = 0;
                                }

                                if (segmentBuffer != null)
                                {
                                    // Saving current data in file buffer
                                    Byte[] tmpBuffer = segmentBuffer;
                                    bufferSize = tmpBuffer.Length + i + s_gZipFileSignature.Length; // compensating

                                    // reallocating file buffer
                                    segmentBuffer = new Byte[bufferSize];

                                    Array.Copy(tmpBuffer, 0, segmentBuffer, 0, tmpBuffer.Length);
                                    Array.Copy(buffer, 0, segmentBuffer, tmpBuffer.Length, i + s_gZipFileSignature.Length);
                                }
                                else
                                {
                                    bufferSize = buffer.Length - segmentStartOffset;
                                    segmentBuffer = new Byte[bufferSize];

                                    Array.Copy(buffer, segmentStartOffset, segmentBuffer, 0, bufferSize);
                                }

                                if (inputStream.Position >= inputStream.Length)
                                {
                                    lock (s_readQueueLocker)
                                    {
                                        ReadQueue.Add(s_readSequenceNumber, segmentBuffer);
                                    }

                                    #region Debug
                                    /*
                                    using (FileStream partFile = new FileStream(@"d:\tmp\detected_compressed_part" + s_readSequenceNumber + ".gz", FileMode.Create))
                                    {
                                        partFile.Write(segmentBuffer, 0, segmentBuffer.Length);
                                    }
                                    */
                                    #endregion

                                    s_readSequenceNumber++;
                                    segmentStartOffset = -1;  // doesn't matter, but still
                                    segmentBuffer = null; // doesn't matter, but still
                                }
                            }
                        }
                    }
                }
            }
            catch (ThreadAbortException)
            {
                // No need to spoil probably existing emergency shutdown message
                s_isEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in Compressed File Read thread caused the process to stop: {0}", ex.Message);
            }

            s_isInputFileRead = true;
        }

        // Universal (de)compressed file write function (threaded)
        private static void FileWriteThread(object parameter)
        {
            s_isOutputFileWritten = false;

            try
            {
                if (parameter == null)
                {
                    throw new ArgumentNullException("parameter", "Output file path for File Write thread is null");
                }

                String fileName = (String)parameter;

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
                            writeQueueItemsCount = WriteQueue.Count;
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
                            isContainsWriteSequenceNumber = WriteQueue.ContainsKey(s_writeSequenceNumber);
                        }
                        if (!isContainsWriteSequenceNumber)
                        {
                            // TODO: fix this
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
                                buffer = WriteQueue[s_writeSequenceNumber];
                                WriteQueue.Remove(s_writeSequenceNumber);

                                if (WriteQueue.Count > s_maxWriteQueueLength)
                                {
                                    s_signalOutputDataQueueReady.Reset();
                                }
                                else
                                {
                                    s_signalOutputDataQueueReady.Set();
                                }
                            }

                            outputStream.Write(buffer, 0, buffer.Length);

                            #region Debug
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
                            #endregion

                            lock (s_writeSequenceNumberLocker)
                            {
                                s_writeSequenceNumber++;
                            }

                            s_signalOutputDataWritten.Set();
                        }

                        Thread.Sleep(1000);
                    }
                }
            }
            catch (ThreadAbortException)
            {
                // No need to spoil probably existing emergency shutdown message
                s_isEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in File Write thread caused the process to stop: {0}", ex.Message);
            }
        }

        // Threaded compression function
        private static void BlockCompressionThread(object parameter)
        {
            byte[] buffer;

            if (s_isEmergencyShutdown)
            {
                return;
            }

            try
            {
                if (parameter == null)
                {
                    throw new ArgumentNullException("parameter", "Thread sequence number for Block Compression thread is null");
                }

                Int64 threadSequenceNumber = (Int64)parameter;    

                lock (s_readQueueLocker)
                {
                    buffer = ReadQueue[threadSequenceNumber];
                    ReadQueue.Remove(threadSequenceNumber);
                }

                // Retry memory allocation flag in case of memory shortage exception
                Boolean isRetryMemoryAllocation = false;
                do
                {
                    try
                    {
                        using (MemoryStream outputStream = new MemoryStream(buffer.Length))
                        {
                            using (GZipStream compressionStream = new GZipStream(outputStream, CompressionMode.Compress))
                            {
                                compressionStream.Write(buffer, 0, buffer.Length);
                                buffer = null;
                            }

                            lock (s_writeQueueLocker)
                            {
                                WriteQueue.Add(threadSequenceNumber, outputStream.ToArray());
                            }
                        }
                    }
                    catch (OutOfMemoryException)
                    {
                        isRetryMemoryAllocation = true;
                        Thread.Sleep(10000); // TODO: replace with a signal from another thread

                    }
                } while (isRetryMemoryAllocation);
                
                
            }
            catch (ThreadAbortException)
            {
                // No need to spoil probably existing emergency shutdown message
                s_isEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in Block Compression thread caused the process to stop: {0}", ex.Message);
            }
        }

        // Threaded decompression function
        private static void BlockDecompressionThread(object parameter)
        {
            byte[] buffer;

            if (s_isEmergencyShutdown)
            {
                return;
            }

            try
            {
                if (parameter == null)
                {
                    throw new ArgumentNullException("parameter", "Thread sequence number for Block Decompression thread is null");
                }

                Int64 threadSequenceNumber = (Int64)parameter;

                lock (s_readQueueLocker)
                {
                    buffer = ReadQueue[threadSequenceNumber];
                    ReadQueue.Remove(threadSequenceNumber);
                }

                // Retry memory allocation flag in case of memory shortage exception
                Boolean isRetryMemoryAllocation = false;
                do
                {
                    try
                    {
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
                                WriteQueue.Add(threadSequenceNumber, decompressedData);
                            }
                            decompressedData = null;
                        }
                    }
                    catch (OutOfMemoryException)
                    {
                        isRetryMemoryAllocation = true;
                        Thread.Sleep(10000); // TODO: replace with a signal from another thread
                    }
                } while (isRetryMemoryAllocation);
            }

            catch (ThreadAbortException)
            {
                // No need to spoil probably existing emergency shutdown message
                s_isEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in Block Decompression thread caused the process to stop: {0}", ex.Message);
            }
        }

        // Threaded threads dispatcher that starts worker threads and limits their number
        private static void WorkerThreadsDispatcherThread()
        {
            if (s_isEmergencyShutdown == true)
            {
                return;
            }

            Int32 readQueueCount = 0;
            try
            {
                do
                {
                    // Killing all running threads is emergency shutdown is requested
                    if (s_isEmergencyShutdown == true)
                    {
                        if (WorkerThreads.Count > 0)
                        {
                            foreach (Int32 threadSequenceNumber in WorkerThreads.Keys)
                            {
                                if (WorkerThreads[threadSequenceNumber].ThreadState == ThreadState.Background ||
                                    WorkerThreads[threadSequenceNumber].ThreadState == ThreadState.Running ||
                                    WorkerThreads[threadSequenceNumber].ThreadState == ThreadState.Suspended ||
                                    WorkerThreads[threadSequenceNumber].ThreadState == ThreadState.WaitSleepJoin)
                                {
                                    WorkerThreads[threadSequenceNumber].Abort();
                                }
                            }
                        }

                        return;
                    }

                    // Looking for finished threads
                    if (WorkerThreads.Count > 0)
                    {
                        List<Int32> finishedThreads = new List<Int32>();
                        foreach (Int32 threadSequenceNumber in WorkerThreads.Keys)
                        {
                            if (WorkerThreads[threadSequenceNumber].ThreadState == ThreadState.Stopped ||
                                WorkerThreads[threadSequenceNumber].ThreadState == ThreadState.Aborted)
                            {
                                finishedThreads.Add(threadSequenceNumber);
                            }
                        }

                        // If any finished threads has been found
                        foreach (Int32 threadSequenceNumber in finishedThreads)
                        {
                            // Removing all of them
                            WorkerThreads.Remove(threadSequenceNumber);
                        }
                    }

                    // If there's less than maximum allowed threads are running, spawn a new one                    
                    if (WorkerThreads.Count < s_maxThreadsCount)
                    {
                        // Spawning a new thread
                        lock (s_readQueueLocker)
                        {
                            foreach (Int64 threadSequenceNumber in ReadQueue.Keys)
                            {
                                if (!WorkerThreads.ContainsKey(threadSequenceNumber))
                                {
                                    // Spawn a corresponding thread according to the selected operations mode
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
                                            throw new Exception("Unknown operations mode is specified");
                                    }

                                    if (workerThread == null)
                                    {
                                        throw new NullReferenceException("Unable to create a block compression/decompression thread");
                                    }

                                    workerThread.Name = String.Format("Data processing (seq: {0})", threadSequenceNumber);
                                    WorkerThreads.Add(threadSequenceNumber, workerThread);
                                    WorkerThreads[threadSequenceNumber].Start(threadSequenceNumber);
                                    break;
                                }
                            }
                        }
                    }
                    else
                    {
                        // If the limit of running worker thread is reached
                        // Stop spawning new threads and wait for the running ones to finish
                        Thread.Sleep(1000);
                    }

                    // Check if there's any block of data in the file read queue
                    lock (s_readQueueLocker)
                    {
                        readQueueCount = ReadQueue.Count;
                    }

                } while (!s_isInputFileRead ||
                         WorkerThreads.Count > 0 ||
                         readQueueCount > 0);    
            }
            catch (ThreadAbortException)
            {
                // No need to spoil probably existing emergency shutdown message
                s_isEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in Worker Threads Dispatcher thread caused the process to stop: {0}", ex.Message);
            }

            s_isDataProcessingDone = true;
        }
        #endregion

        #region FUNCTIONS / PROCEDURES
        // Initialize internal variables
        private static void Initialize()
        {
            if (s_isEmergencyShutdown)
            {
                return;
            }

            try
            {
                // Initializing threads
                s_workerThreadsManagerThread = null;
                s_inputFileReadThread = null;
                s_outputFileWriteThread = null;

                // Initializing flags
                s_isInputFileRead = false;
                s_isDataProcessingDone = false;
                s_isOutputFileWritten = false;
                s_isEmergencyShutdown = false;

                // Cleaning emergency shutdown message
                s_emergencyShutdownMessage = "";

                // Resetting sequence numbers
                s_readSequenceNumber = 0;
                s_writeSequenceNumber = 0;

                // Evaluating maximum threads count
                s_maxThreadsCount = Environment.ProcessorCount - s_maxThreadsSubtraction;
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception during compression module initialization caused the process to stop: {0}", ex.Message);
            }
        }

        // Execute main compression / decompression logic
        public static void Run(String inputFilePath, String outputFilePath)
        {
            try
            {
                Initialize();

                if (s_isEmergencyShutdown)
                {
                    return;
                }

                // Starting compression threads manager thread
                s_workerThreadsManagerThread = new Thread(WorkerThreadsDispatcherThread);
                s_workerThreadsManagerThread.Name = "Worker threads manager";
                s_workerThreadsManagerThread.Start();

                // Initializing input file read thread
                switch (s_compressionMode)
                {
                    case CompressionMode.Compress:
                        // Starting file reader thread
                        s_inputFileReadThread = new Thread(FileReadUncompressedThread);
                        s_inputFileReadThread.Name = "Read uncompressed input file";
                        break;

                    case CompressionMode.Decompress:
                        // Starting file reader thread
                        s_inputFileReadThread = new Thread(FileReadCompressedThread);
                        s_inputFileReadThread.Name = "Read compressed input file";
                        break;

                    default:
                        throw new Exception("Unknown operations mode is specified");
                }

                // Starting previously initialized input file read thread
                s_inputFileReadThread.Start(inputFilePath);

                // Starting compressed file write thread
                s_outputFileWriteThread = new Thread(FileWriteThread);
                s_outputFileWriteThread.Name = "Write output file";
                s_outputFileWriteThread.Start(outputFilePath);

                // Awaiting for all thread to complete while being able to kill them if an exception is occured
                while (!s_isInputFileRead || 
                       !s_isDataProcessingDone ||
                       !s_isOutputFileWritten)
                {
                    // Killing all the threads that was started here if emergency shutdown is requested
                    if (s_isEmergencyShutdown)
                    {
                        if (s_inputFileReadThread != null)
                        {
                            s_inputFileReadThread.Abort();
                        }

                        if (s_outputFileWriteThread != null)
                        {
                            s_outputFileWriteThread.Abort();
                        }

                        if (s_workerThreadsManagerThread != null)
                        {
                            s_workerThreadsManagerThread.Abort();
                        }

                        break;
                    }

                    Thread.Sleep(10000);
                }
            }
            catch (Exception ex)
            {
                s_isEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in compression method caused the process to stop: {0}", ex.Message);
            }
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
        static void PrintUsage()
        {
            String appName;
            if (String.IsNullOrEmpty(System.AppDomain.CurrentDomain.FriendlyName))
            {
                appName = "app.exe";
            }
            else
            {
                appName = System.AppDomain.CurrentDomain.FriendlyName;
            }
            String message = String.Format(
                "\nThis program implements multi-threaded compresses and decompresses for a specified file using GZipStream class from .NET 3.5\n\n" +
                "Usage:\n" +
                "{0} MODE SOURCE DESTINATION\n\n" +
                "MODEs:\n" +
                "  compress – pack a file\n" + 
                "    SOURCE - original file path\n" +
                "    DESTINATION - compressed file path\n\n" + 
                "  decompress – unpack an archive\n" + 
                "    SOURCE - compressed file path\n" +
                "    DESTINATION - decompressed file path\n\n" + 
                "Examples:\n" +
                "{0} compress \"c:\\Documents\\iis.log\" \"c:\\Documents\\iis.log.gz\"\n" +
                "{0} decompress \"c:\\Documents\\iis.log.gz\" \"c:\\Documents\\iis.log\"\n\n",
                appName);

            Console.Write(message);
        }

        static int Main(string[] args)
        {
            String inputFilePath;
            String outputFilePath;

            try
            {
                #region Checking arguments
                // If "help" is requested
                if (args != null &&
                    args.Length > 0 &&
                    String.Compare(args[0], "help", true) == 0)
                {
                    PrintUsage();
                    return 0;
                }

                if (args == null ||
                    args.Length != 3)
                {
                    // If less or more than 3 arguments are specefied, print an error message
                    String message = String.Format("Incorrect number of parameters (expected: 3, got {0})\n", args.Length);
                    throw new ArgumentException(message);
                }

                // Checking operations mode switch
                String mode = args[0];
                if (String.Compare(mode, "compress", true) == 0)
                {
                    // Setting compression mode
                    Console.WriteLine("Compression mode is specified.");
                    CGZipCompressor.CompressionMode = CompressionMode.Compress;
                }
                else if (String.Compare(mode, "decompress", true) == 0)
                {
                    // Setting decompression mode
                    Console.WriteLine("Decompression mode is specified.");
                    CGZipCompressor.CompressionMode = CompressionMode.Decompress;
                }
                else
                {
                    // Unknown mode is spceified
                    String message = String.Format("Incorrect mode specified (expected: \"compress\" or \"decompress\" or \"help\", got \"{0}\")\n", mode);
                    throw new ArgumentException(message);
                }

                // Checking input file
                inputFilePath = args[1];
                if (!File.Exists(inputFilePath))
                {
                    // Input file must exists
                    String message = String.Format("Can't find the specified input file \"{0}\"\n", inputFilePath);
                    throw new ArgumentException(message);
                }

                outputFilePath = args[2];
                if (File.Exists(outputFilePath))
                {
                    // Output file mustn't exists
                    String message = String.Format("The specified output file is already exists: \"{0}\"\n", outputFilePath);
                    throw new ArgumentException(message);
                }


                #endregion

                #region DEBUG
                //String inputFilePath = @"c:\tmp\Iteration4-2x4CPU_16GB_RAM.blg";
                //String inputFilePath = @"c:\tmp\uncompressed-files-archive.zip";
                //String inputFilePath = @"c:\tmp\";
                //String inputFilePath = @"e:\Downloads\Movies\Imaginaerum.2012.1080p.BluRay.x264.YIFY.mp4";
                //String inputFilePath = @"e:\Downloads\Movies\Mad.Max.Fury.Road.2015.1080p.BluRay.AC3.x264-ETRG.mkv";
                //FileInfo inputFileInfo = new FileInfo(inputFilePath);
                //String compressedFilePath = inputFileInfo.FullName + ".gz";
                //String decompressedFilePath = inputFileInfo.Directory + @"\" + inputFileInfo.Name.Replace(inputFileInfo.Extension, null) + " (1)" + inputFileInfo.Extension;

                //CGZipCompressor.Run(inputFilePath, compressedFilePath, CompressionMode.Compress);
                //CGZipCompressor.Run(compressedFilePath, decompressedFilePath, CompressionMode.Decompress);
                #endregion

                #region Starting compression
                Console.WriteLine("Working...");
                CGZipCompressor.Run(inputFilePath, outputFilePath);

                if (CGZipCompressor.IsEmergencyShutdown)
                {
                    String message = String.Format("The compression process was aborted because of the following error: {0}", CGZipCompressor.EmergenceShutdownMessage);
                    throw new Exception(message);
                }
                else
                {
                    Console.WriteLine("The process has been completed successfully");
                    Console.WriteLine("\nPress ENTER to exit...");
                    Console.ReadLine();

                    return 0;
                }
                #endregion
            }
            catch (Exception ex)
            {
                String message = String.Format("\nERROR: {0}", ex.Message);
                Console.WriteLine(message);
                Console.WriteLine("\nPress ENTER to exit...");
                Console.ReadLine();

                return 1;
            }
        }
    }
}
