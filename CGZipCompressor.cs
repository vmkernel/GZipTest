using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;

// TODO: check null variables
// TODO: add file format check in order to prevent decompression of a uncompressed file

namespace GZipTest
{
    public static class CGZipCompressor
    {
        #region HARDCODED SETTINGS
        // Size in bytes of a compression block from a input uncompressed file
        // An input file is split to blocks with the specified size and each block is compressed separately by a block compression thread
        // FEATURE: It might be better decision to evaluated the size dynamically, according to
        //  * input file size 
        //  * number of CPU cores in the system 
        //  * amount of free RAM in the system
        // With maximum limit in order to prevent RAM drain and to allow another system with less amount of RAM to decompress the file 
        private static readonly Int32 s_compressionBlockSize = 128 * 1024 * 1024;

        // Maximum lenght of read queue.
        // After reaching this value file read thread will wait until data from read queue will be processed by worker thread(s)
        private static readonly Int32 s_maxProcessingQueueLength = 4;

        // Maximum lenght of write queue. 
        // After reaching this value file read thread will wait until data from write queue will be written to output file
        private static readonly Int32 s_maxWriteQueueLength = 4;

        // Reserves the specified number of CPU cores for the running operations system
        // If the value is 0, then no reservation applies
        // If the value is 1, then one CPU core is kept for the system to run more smoothly and responsively to a user's actions (and so on)
        private static readonly Int32 s_cpuReservation = 1;
        #endregion

        #region FIELDS
        // TODO: check gzipped block signature before decompressing a block
        // First three bytes of a GZip file (compressed block) signature
        //private static readonly Byte[] s_gZipFileSignature = new Byte[] { 0x1F, 0x8B, 0x08 };

        // Operations mode (compression or decompression)
        public static CompressionMode CompressionMode { get; set; }

        #region Emergency shutdown
        // Emergency shutdown flag
        // Indicates an unexpected error and all running threads will exit as soon as possible
        public static Boolean IsEmergencyShutdown { get; private set; }

        // Emergency shutdown message
        // When IsEmergencyShutdown flas is set, stores a description of an error that has caused the error
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
        // Worker threads (compression / decompression) pool
        private static Dictionary<Int32, Thread> s_workerThreads;
        private static Dictionary<Int32, Thread> WorkerThreads
        {
            get
            {
                if (s_workerThreads == null)
                {
                    s_workerThreads = new Dictionary<Int32, Thread>();
                }
                return s_workerThreads;
            }

            set
            {
                if (s_workerThreads == null)
                {
                    s_workerThreads = new Dictionary<Int32, Thread>();
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

        // Worker threads pool inter-thread locker
        private static readonly Object s_workerThreadsLocker = new Object();

        // Maximum worker threads count
        private static Int32 s_maxThreadsCount;
        #endregion

        // Worker threads manager thread
        private static Thread s_workerThreadsManagerThread;

        // Input file read thread
        private static Thread s_inputFileReadThread;

        // Output file write thread
        private static Thread s_outputFileWriteThread;
        #endregion

        #region Block sequences management
        // Read sequence number
        // Indicates a sequence number of current block that has been read from an input file
        private static Int32 s_readSequenceNumber;

        // Read sequence number
        // Indicates a sequence number of current block that has to be written to an output file
        private static Int32 s_writeSequenceNumber;
        #endregion

        #region Queues
        #region Universal block processing queue
        // Stores blocks of data that has been read from an input file until they are picked up by a Block Processing thread
        private static Dictionary<Int32, Object> s_blockProcessingQueue;
        private static Dictionary<Int32, Object> BlockProcessingQueue
        {
            get
            {
                if (s_blockProcessingQueue == null)
                {
                    s_blockProcessingQueue = new Dictionary<Int32, Object>();
                }
                return s_blockProcessingQueue;
            }

            set
            {
                if (s_blockProcessingQueue == null)
                {
                    s_blockProcessingQueue = new Dictionary<Int32, Object>();
                }
                if (value == null)
                {
                    s_blockProcessingQueue.Clear();
                }
                else
                {
                    s_blockProcessingQueue = value;
                }
            }
        }

        // Block processing queue locker
        private static readonly Object s_blockProcessingQueueLocker = new Object();
        #endregion

        #region Universal block write queue
        // Stores processed blocks of data which are produced by Block Processing thread until they are picked up by the output file write thread
        private static Dictionary<Int32, Object> s_blockWriteQueue;
        private static Dictionary<Int32, Object> BlockWriteQueue
        {
            get
            {
                if (s_blockWriteQueue == null)
                {
                    s_blockWriteQueue = new Dictionary<Int32, Object>();
                }
                return s_blockWriteQueue;
            }

            set
            {
                if (s_blockWriteQueue == null)
                {
                    s_blockWriteQueue = new Dictionary<Int32, Object>();
                }
                if (value == null)
                {
                    s_blockWriteQueue.Clear();
                }
                else
                {
                    s_blockWriteQueue = value;
                }
            }
        }

        // Block write queue locker
        private static readonly Object s_blockWriteQueueLocker = new Object();
        #endregion
        #endregion

        #region Flags
        // Flag: the input file has been read and the file read thread has exited
        private static Boolean s_isInputFileRead;

        // Flag: all input data blocks has been processed and all worker threads are terminated
        private static Boolean s_isDataProcessingDone;

        // Flag: the output file has been written and file write thread has exited
        private static Boolean s_isOutputFileWritten;
        #endregion

        #region Inter-thread communication signals
        // Is used for throttling input file read when input file read queue becames longer than specified in s_maxReadQueueLength
        // Fires when a file write thread completes writing of a processed data block
        private static EventWaitHandle s_signalOutputDataWritten = new EventWaitHandle(false, EventResetMode.AutoReset);

        // Is used for throttling input file read when output file write queue becames longer than specified in s_maxWriteQueueLength
        private static ManualResetEvent s_signalOutputDataQueueReady = new ManualResetEvent(false);
        #endregion
        #endregion

        #region THREADS DEFINITION
        // Thread: Universal file read function
        private static void FileReadThread(object parameter)
        {
            // Resetting "file is read" flag
            s_isInputFileRead = false;

            try
            {
                // Checking file path parameter and converting it from object to string
                if (parameter == null)
                {
                    throw new ArgumentNullException("parameter", "Input file path for the File Read thread is null.");
                }
                String fileName = (String)parameter;

                // Reading the file
                using (FileStream inputStream = new FileStream(fileName, FileMode.Open, FileAccess.Read))
                {
                    while (inputStream.Position < inputStream.Length)
                    {
                        // Throtling read of the file until the file writer thread signals that output data queue is ready to receive data
                        s_signalOutputDataQueueReady.WaitOne();

                        // Throttling read of the file to avoid memory drain (controlled by s_maxReadQueueLength variable)
                        Int32 blockProcessingQueueItemsCount;
                        lock (s_blockProcessingQueueLocker)
                        {
                            blockProcessingQueueItemsCount = BlockProcessingQueue.Count;
                        }

                        if (blockProcessingQueueItemsCount >= s_maxProcessingQueueLength)
                        {
                            // If maximum allowed compression queue length is reached
                            // Suspending the read thread until the file writer thread signals that a block of data has been written to the output file (or the timeout expires)
                            s_signalOutputDataWritten.WaitOne(10000);

                            // And re-evaluating the length of the compression queue
                            continue;
                        }

                        Int32 bytesRead; // Number of bytes read from the input file
                        if (CompressionMode == CompressionMode.Compress)
                        {
                            #region Reading uncompressed file for compression
                            // Calculating read buffer size
                            Int32 bufferSize;

                            // It the residual lenght of the file is less than the predefined read buffer size
                            if ((inputStream.Length - inputStream.Position) < s_compressionBlockSize)
                            {
                                // Set the actual buffer size to the residual lenght of the file
                                bufferSize = (Int32)(inputStream.Length - inputStream.Position);
                            }
                            // If the residual lenght of the file is greather or equal to the predefined read buffer size
                            else
                            {
                                // Use the predefined buffer size
                                bufferSize = s_compressionBlockSize;
                            }

                            // Handling calculations error
                            if (bufferSize <= 0)
                            {
                                throw new IndexOutOfRangeException("Unable to calculate read block size. Current position in the input stream might be beyond the end of the input file.");
                            }

                            // Allocating read buffer and reading a block from the file                        
                            Byte[] buffer = new Byte[bufferSize];
                            bytesRead = inputStream.Read(buffer, 0, buffer.Length);

                            // If no bytes has been read or if the number of actually read bytes doesn't match the expected number of bytes
                            if (bytesRead <= 0 ||
                                bytesRead != bufferSize)
                            {
                                // Assuming an error 
                                throw new InvalidDataException("An attemp to read from input file stream has returned unexpected number of read bytes.");
                            }

                            lock (s_blockProcessingQueueLocker)
                            {
                                // Adding the read block to the compression queue
                                BlockProcessingQueue.Add(s_readSequenceNumber, buffer);
                            }
                            #endregion
                        }
                        else if (CompressionMode == CompressionMode.Decompress)
                        {
                            #region Reading compressed file for decompression
                            //Creating GZip-compressed block object
                            CGZipBlock gZipBlock = new CGZipBlock();

                            // Allocating buffer and reading metadata form the input file
                            Byte[] metadataBuffer = new byte[CGZipBlock.MetadataSize];
                            bytesRead = inputStream.Read(metadataBuffer, 0, metadataBuffer.Length);

                            // If no bytes has been read or if the number of actually read bytes doesn't match the expected number of bytes
                            if (bytesRead <= 0 ||
                                bytesRead != CGZipBlock.MetadataSize)
                            {
                                // Assuming an error 
                                throw new InvalidDataException("An attemp to read compressed block's metadate from the input file stream has returned unexpected number of read bytes.");
                            }

                            // Initializing GZip-compressed block object with metadata
                            gZipBlock.InitializeWithMetadata(metadataBuffer);

                            // Allocating buffer and reading compressed block from the input file
                            Byte[] compressedDataBuffer = new Byte[gZipBlock.DataSizeCompressed];
                            bytesRead = inputStream.Read(compressedDataBuffer, 0, compressedDataBuffer.Length);

                            // If no bytes has been read or if the number of actually read bytes doesn't match the expected number of bytes
                            if (bytesRead <= 0 ||
                                bytesRead != gZipBlock.DataSizeCompressed)
                            {
                                // Assuming an error 
                                throw new InvalidDataException("An attemp to read compressed block from the input file stream has returned unexpected number of read bytes.");
                            }

                            // Assigning the read compressed block to the corresponding section of GZip-compressed block
                            gZipBlock.Data = compressedDataBuffer;

                            lock (s_blockProcessingQueueLocker)
                            {
                                // Adding the read block to the decompression queue
                                BlockProcessingQueue.Add(s_readSequenceNumber, gZipBlock);
                            }
                            #endregion
                        }
                        else
                        {
                            throw new Exception("Unknown operations mode is specified");
                        }

                        // Incrementing read sequence counter
                        s_readSequenceNumber++;
                    }
                }
            }
            catch (ThreadAbortException)
            {
                // No need to overwrite probably existing emergency shutdown message, just setting the emergency shutdown flag
                IsEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                // Setting the emergency shutdown flag and generating error message
                IsEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in File Read thread caused the process to stop: {0}", ex.Message);
            }

            // Setting "file is read" flag
            s_isInputFileRead = true;
        }

        // Thread: Universal file write function
        private static void FileWriteThread(object parameter)
        {
            // Resetting "file is read" flag
            s_isOutputFileWritten = false;

            try
            {
                // Checking file path parameter and converting it from object to string
                if (parameter == null)
                {
                    throw new ArgumentNullException("parameter", "Output file path for File Write thread is null");
                }
                String fileName = (String)parameter;

                // Writing the file
                using (FileStream outputStream = new FileStream(fileName, FileMode.Create))
                {
                    // Initial signal to the file read thread to allow reading
                    s_signalOutputDataQueueReady.Set();

                    while (!s_isOutputFileWritten) // Don't required, but it's a good fail-safe measure instead of using "while (true)"
                    {
                        // Checking if there's any data in output queue that ready to be written
                        Int32 writeQueueItemsCount;
                        lock (s_blockWriteQueueLocker)
                        {
                            writeQueueItemsCount = BlockWriteQueue.Count;
                        }

                        // If the number of blocks in the write queue is greatherthan maximum allowed
                        if (writeQueueItemsCount > s_maxWriteQueueLength)
                        {
                            // Signalling to the file read thread to pause reading
                            s_signalOutputDataQueueReady.Reset();
                        }
                        else
                        {
                            // If lower than maximum allowed then signalling to the file read thread to resume reading
                            s_signalOutputDataQueueReady.Set();
                        }

                        if (writeQueueItemsCount <= 0)
                        {
                            // If all source blocks has been read and processed
                            if (s_isDataProcessingDone)
                            {
                                // Setting output file is written flag and breaking the loop
                                s_isOutputFileWritten = true;
                                break;
                            }
                        }

                        // Checking if there's a block of output data with the same sequence number as the write sequence number
                        Boolean isContainsWriteSequenceNumber = false;
                        lock (s_blockWriteQueueLocker)
                        {
                            isContainsWriteSequenceNumber = BlockWriteQueue.ContainsKey(s_writeSequenceNumber);
                        }
                        
                        if (!isContainsWriteSequenceNumber)
                        {
                            // TODO: Implement suspend
                            // If there's no block with correct write sequence number the the queue, wait for the next block and go round the loop
                            //s_signalWorkerThreadReady.WaitOne();
                            //lock (s_workerThreadReadySignalLocker)
                            //{
                            //    s_signalWorkerThreadReady.Reset();
                            //}
                        }
                        // If there's a block with correct write sequence number, write it to the output file
                        else
                        {
                            Byte[] buffer;
                            if (CompressionMode == CompressionMode.Compress)
                            {
                                CGZipBlock compressedBlock;
                                lock (s_blockWriteQueueLocker)
                                {
                                    // Moving a compressed block from write queue to local "buffer"
                                    compressedBlock = BlockWriteQueue[s_writeSequenceNumber] as CGZipBlock;
                                    BlockWriteQueue.Remove(s_writeSequenceNumber);
                                }

                                // Converting GZip-block object to byte array to be able to write it to the output file
                                buffer = compressedBlock.ToByteArray();
                            }
                            else if (CompressionMode == CompressionMode.Decompress)
                            {
                                lock (s_blockWriteQueueLocker)
                                {
                                    buffer = BlockWriteQueue[s_writeSequenceNumber] as Byte[];
                                    BlockWriteQueue.Remove(s_writeSequenceNumber);
                                }
                            }
                            else
                            {
                                throw new Exception("Unknown operations mode is specified");
                            }
                            
                            // Writing a GZip-block to the file
                            outputStream.Write(buffer, 0, buffer.Length);

                            // Incrementing write sequence counter
                            s_writeSequenceNumber++;

                            // Signalling to the file read thread that a block of data has been written to the ouput file
                            s_signalOutputDataWritten.Set();
                        }

                        // TODO: replace with signalling
                        Thread.Sleep(1000);
                    }
                }
            }
            catch (ThreadAbortException)
            {
                // No need to overwrite probably existing emergency shutdown message, just setting the emergency shutdown flag
                IsEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                // Setting the emergency shutdown flag and generating error message
                IsEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in File Write thread caused the process to stop: {0}", ex.Message);
            }
        }

        // Thread: Universal block processing function
        private static void BlockProcessingThread(object parameter)
        {
            try
            {
                #region Checking argument
                // Checking and receiving thread sequence number from input object
                if (parameter == null)
                {
                    throw new ArgumentNullException("parameter", "Thread sequence number for Block Processing thread is null.");
                }

                Int32 threadSequenceNumber = (Int32)parameter;
                if (threadSequenceNumber < 0)
                {
                    throw new ArgumentOutOfRangeException("Thread sequence number for Block Processing thread is less than 0.");
                }
                #endregion

                // Allocating buffers for (de)compression modes
                Byte[] buffer = new Byte[0]; // Input data buffer for compression mode
                CGZipBlock gZipBlock = new CGZipBlock(); // Input compressed block for decompression mode

                #region Retriving input data from read queue
                // Compressing if requested
                if (CompressionMode == CompressionMode.Compress)
                {
                    lock (s_blockProcessingQueueLocker)
                    {
                        // Moving a block of uncompressed data from read queue to a local buffer
                        buffer = BlockProcessingQueue[threadSequenceNumber] as Byte[];
                        BlockProcessingQueue.Remove(threadSequenceNumber);
                    }
                }
                // Decompressing if requested
                else if (CompressionMode == CompressionMode.Decompress)
                {
                    // Getting unprocessed data from read queue to internal buffer
                    lock (s_blockProcessingQueue)
                    {
                        // Moving a block of compressed data from read queue to a local buffer
                        gZipBlock = BlockProcessingQueue[threadSequenceNumber] as CGZipBlock;
                        BlockProcessingQueue.Remove(threadSequenceNumber);
                    }
                }
                else
                {
                    throw new Exception("Unknown operations mode is specified");
                }
                #endregion

                // Retry memory allocation flag in case of OutOfMemory exception
                Boolean isRetryMemoryAllocation = false;
                do
                {
                    try
                    {
                        #region Processing the received block
                        if (CompressionMode == CompressionMode.Compress)
                        {
                            // Allocating memory stream that will receive the compressed data block
                            using (MemoryStream outputStream = new MemoryStream(buffer.Length))
                            {
                                // Compressing the data block
                                using (GZipStream compressionStream = new GZipStream(outputStream, CompressionMode.Compress))
                                {
                                    compressionStream.Write(buffer, 0, buffer.Length);
                                }

                                // Storing the compressed data block in a GZip block object with metadata
                                CGZipBlock compressedBlock = new CGZipBlock();
                                compressedBlock.Data = outputStream.ToArray();
                                compressedBlock.DataSizeUncompressed = buffer.Length;

                                lock (s_blockWriteQueueLocker)
                                {
                                    // Placing the block of data and metadata to the write queue
                                    BlockWriteQueue.Add(threadSequenceNumber, compressedBlock);
                                }
                            }
                        }
                        else if (CompressionMode == CompressionMode.Decompress)
                        {
                            // Creating memory stream from unprocessed data
                            using (MemoryStream inputStream = new MemoryStream(gZipBlock.Data))
                            {
                                Int32 bytesRead;

                                // Allocating output buffer according to pre-defined chunk size
                                Byte[] outputBuffer = new Byte[gZipBlock.DataSizeUncompressed];
                                if (outputBuffer.Length <= 0)
                                {
                                    throw new ArgumentOutOfRangeException("Uncompressed data buffer's size is less or equal to 0.");
                                }

                                // Decompressing the data block
                                using (GZipStream gZipStream = new GZipStream(inputStream, CompressionMode.Decompress))
                                {
                                    bytesRead = gZipStream.Read(outputBuffer, 0, outputBuffer.Length);
                                }
                                if (bytesRead <= 0 ||
                                    bytesRead != outputBuffer.Length)
                                {
                                    // Assuming an error 
                                    throw new InvalidDataException("An attemp to read from compressed data block has returned unexpected number of read bytes.");
                                }

                                lock (s_blockWriteQueueLocker)
                                {
                                    // Putting processed data block to File Write queue
                                    BlockWriteQueue.Add(threadSequenceNumber, outputBuffer);
                                }
                            }
                        }
                        else
                        {
                            throw new Exception("Unknown operations mode is specified");
                        }
                        #endregion
                    }
                    catch (OutOfMemoryException)
                    {
                        // Handling OutOfMemory exception with sleep and retry
                        isRetryMemoryAllocation = true;
                        Thread.Sleep(10000); // TODO: replace with a signal from another thread

                    }
                } while (isRetryMemoryAllocation);
            }
            catch (ThreadAbortException)
            {
                // No need to overwrite probably existing emergency shutdown message, just setting the emergency shutdown flag
                IsEmergencyShutdown = true;
            }
            catch (Exception ex)
            {
                // Setting the emergency shutdown flag and generating error message
                IsEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in Block Compression thread caused the process to stop: {0}", ex.Message);
            }
        }

        // Threaded threads dispatcher that starts worker threads and limits their number
        private static void WorkerThreadsDispatcherThread()
        {
            // Resetting "all data has been processed" flag
            s_isDataProcessingDone = false;

            // Declaring one of the variables that is used to trigger exit from do-while look
            Int32 blockProcessingQueueItemsCount = 0;
            try
            {
                do
                {
                    #region Removing finished treads from the pool
                    if (WorkerThreads.Count > 0)
                    {
                        // Looking for finished threads and removing them from the pool
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
                            // Removing all of them from the pool
                            WorkerThreads.Remove(threadSequenceNumber);
                        }
                    }
                    #endregion

                    // If there's less than maximum allowed threads are running, spawn a new one                    
                    if (WorkerThreads.Count < s_maxThreadsCount)
                    {
                        #region Starting a new block processing thread
                        // Declaring a block processing thread
                        Thread workerThread = new Thread(BlockProcessingThread);


                        lock (s_blockProcessingQueueLocker)
                        {
                            // Going through all sequence numbers in the block processin queue
                            foreach (Int32 threadSequenceNumber in BlockProcessingQueue.Keys)
                            {
                                // If there's no running thread for the corrsponding block sequence number in the tread pool
                                if (!WorkerThreads.ContainsKey(threadSequenceNumber))
                                {
                                    // Starting a new block processing thread for the corresponding block
                                    if (CompressionMode == CompressionMode.Compress)
                                    {
                                        workerThread.Name = String.Format("Block compression (seq: {0})", threadSequenceNumber);
                                    }
                                    else if (CompressionMode == CompressionMode.Decompress)
                                    {
                                        workerThread.Name = String.Format("Block decompression (seq: {0})", threadSequenceNumber);
                                    }
                                    else
                                    {
                                        throw new Exception("Unknown operations mode is specified");
                                    }
                                    WorkerThreads.Add(threadSequenceNumber, workerThread);
                                    WorkerThreads[threadSequenceNumber].Start(threadSequenceNumber);
                                    break;
                                }
                            }
                        }
                        #endregion
                    }
                    else
                    {
                        // If the limit of running worker thread is reached
                        // Stop spawning new threads and wait for the running ones to finish
                        Thread.Sleep(1000);
                    }

                    // Check if there's any block of data in the file read queue
                    lock (s_blockProcessingQueueLocker)
                    {
                        blockProcessingQueueItemsCount = BlockProcessingQueue.Count;
                    }

                    // Triggering thread management loop exit when
                    // * Input file has been read
                    // * Read queue is empty
                    // * There's no running block processing threads
                } while (!s_isInputFileRead ||
                         WorkerThreads.Count > 0 ||
                         blockProcessingQueueItemsCount > 0);
            }
            catch (ThreadAbortException)
            {
                // No need to overwrite probably existing emergency shutdown message, just setting the emergency shutdown flag
                IsEmergencyShutdown = true;

                #region Sending Abort() signal to all running threads if emergency shutdown is requested
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
                #endregion
            }
            catch (Exception ex)
            {
                // Setting the emergency shutdown flag and generating error message
                IsEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in Worker Threads Dispatcher thread caused the process to stop: {0}", ex.Message);
            }

            // Setting "all data has been processed" flag
            s_isDataProcessingDone = true;
        }
        #endregion

        #region OTHER FUNCTIONS / PROCEDURES
        // Initialize internal variables
        private static void Initialize()
        {
            try
            {
                // Initializing threads
                s_workerThreadsManagerThread = null;
                s_inputFileReadThread = null;
                s_outputFileWriteThread = null;

                // Initializing emergency shutdown
                IsEmergencyShutdown = false;
                s_emergencyShutdownMessage = "";

                // Resetting sequence numbers
                s_readSequenceNumber = 0;
                s_writeSequenceNumber = 0;

                // Evaluating maximum threads count
                if (s_cpuReservation < 0)
                {
                    // If the value is incorrect ignoring it
                    s_maxThreadsCount = Environment.ProcessorCount;
                }
                else if (s_cpuReservation >= Environment.ProcessorCount)
                {
                    // If the value is greather or equal to CPU cores count, setting worker threads limit to 1 to be able to run at least one worker thread
                    s_maxThreadsCount = 1;
                }
                else
                {
                    // On all the other cases simply subtracting CPU reservation from total number of CPU cores of the system
                    s_maxThreadsCount = Environment.ProcessorCount - s_cpuReservation;
                }
            }
            catch (Exception ex)
            {
                // Setting the emergency shutdown flag and generating error message
                IsEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception during compression module initialization caused the process to stop: {0}", ex.Message);
            }
        }

        // Execute main compression / decompression logic
        public static void Run(String inputFilePath, String outputFilePath)
        {
            try
            {
                Initialize();

                if (IsEmergencyShutdown)
                {
                    return;
                }

                // Starting compression threads manager thread
                s_workerThreadsManagerThread = new Thread(WorkerThreadsDispatcherThread);
                s_workerThreadsManagerThread.Name = "Worker threads manager";
                s_workerThreadsManagerThread.Start();

                // Initializing input file read and write threads
                s_inputFileReadThread = new Thread(FileReadThread);
                s_outputFileWriteThread = new Thread(FileWriteThread);
                if (CompressionMode == CompressionMode.Compress)
                {
                    s_inputFileReadThread.Name = "Uncompressed file reader";
                    s_outputFileWriteThread.Name = "Compressed file writer";
                }
                else if (CompressionMode == CompressionMode.Decompress)
                {
                    s_inputFileReadThread.Name = "Compressed file reader";
                    s_outputFileWriteThread.Name = "Decompressed file writer";
                }
                else
                {
                    throw new Exception("Unknown operations mode is specified");
                }

                // Starting previously initialized input file read thread
                s_inputFileReadThread.Start(inputFilePath);

                // Starting compressed file write thread
                s_outputFileWriteThread.Start(outputFilePath);

                // Awaiting for all thread to complete while being able to kill them if an exception is occured
                while (!s_isInputFileRead ||
                       !s_isDataProcessingDone ||
                       !s_isOutputFileWritten)
                {
                    // Killing all the threads that has been started if emergency shutdown is requested
                    if (IsEmergencyShutdown)
                    {
                        if (s_workerThreadsManagerThread != null)
                        {
                            s_workerThreadsManagerThread.Abort();
                        }

                        if (s_outputFileWriteThread != null)
                        {
                            s_outputFileWriteThread.Abort();
                        }

                        if (s_inputFileReadThread != null)
                        {
                            s_inputFileReadThread.Abort();
                        }

                        // Waiting for the threads to abort
                        s_workerThreadsManagerThread.Join();
                        s_outputFileWriteThread.Join();
                        s_inputFileReadThread.Join();                      
                        break;
                    }

                    Thread.Sleep(10000);
                }
            }
            catch (Exception ex)
            {
                // Setting the emergency shutdown flag and generating error message
                IsEmergencyShutdown = true;
                s_emergencyShutdownMessage = String.Format("An unhandled exception in compression method caused the process to stop: {0}", ex.Message);
            }
        }
        #endregion
    }
}
