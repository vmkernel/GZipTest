# Objectives
* Develop a command line tool for compressing and decompressing files using class System.IO.Compression.GzipStream.
* The program should effectively work in a multicore environment and should be able to process files, which are bigger than total RAM size.
* Program code must be safe and robust in terms of exceptions.
* Please use only standard libraries from .NET Framework 3.5 to work with multithreading.
*  ability to stop program correctly by Ctrl-C would be a plus.
* On successful result program should return 0, otherwise 1.

Use the following command line arguments:
* compressing: GZipTest.exe compress [original file name] [archive file name]
* decompressing: GZipTest.exe decompress [archive file name] [decompressing file name]

Please send us solution source files and Visual Studio project. Briefly describe architecture and algorithms used.



# Program description
Main business-logic of the application is held within static class CGZipCompressor.

Compression mode is set by the property CGZipCompressor.CompressionMode (System.IO.Compression) and might take only two values: Compress and Decompress. In case if the mode somehow receives another value that doesn't equal to this thow, the program will throw an exception.


## Settings
The program has the following hardcoded settings:
* Compression block size
* Maximal length of processing queue
* Maximal lenght of write queue
* Number of CPU cores which are reserved for operating system's needs


## Exceptions handling
**TBD**
In case of any exception in any thread the program will set the IsEmergencyShutdown flag and store the exception's message along with soeme details in EmergencyShutdownMessage string variable. This message will be displayed to a user.


## Processing stages
The whole process consist of this three stages:
1. Reading blocks of data from an input file
1. Processing the blocks
1. Writing the processed blocks to an output file

### Stage completion flags
There's three flags that informs the whole compressor class of its current state
* s_isInputFileRead - Indicates that an input file has been read to the end and the file read thread has exited.
* s_isDataProcessingDone - Indicates that all input data blocks has been processed and all worker threads are terminated.
* s_isOutputFileWritten - Indicates that the output file has been written and file write thread has exited.

When all of this three flags are set to true, it means that the whole process has been finished.


## Reading input and writing output files
The basic idea for the processes is queues and read/write sequence numbers. 

### File formats
For an uncompressed file everything is pretty simple - it's just a file as is.
A compressed file consists of blocks described by CGZipBlock object that contains:
* Metadata: uncompressed (original) block size (to be able to preallocate a buffer for decompression) and compressed block size (to be able to read the compressed block from the file.
* Compressed data block which size is stored in the metadata.

### Queues
There are two queues:
1. Processing queue (stores block of input file until they are processed by a worker thread)
1. File write queue (stores the processed blocks until they are written to an output file)

Each queue is represented by a Dictionary<Int32, Object> with block's sequence number as a key and a block of data storead as an object value. 

The final type of this Object variable depends on the selected operation mode. 
If compression mode is selected then it's byte array (Byte[]) in processing queue and CGZipBlock object in file write queue.
If decompresion mode is selected then it's reversed: GZipBlock in processing queue and byte array (Byte[]) in file write queue.

The length of each queue is limited by s_maxProcessingQueueLength and s_maxWriteQueueLength variables in order to prevent RAM drain. When one of these limits is reached the file read thread suspends further read operations until both of the queues have at least one free slot for a data block.

Every item in the queues consists of the data block and its sequence number which is assigned to a block when it's read from an input file. The sequence number stays with the block of data until it's written to an output file. 
All blocks of an input file are read to the processing queue. From the processing queue each block is picked up by a block processing thread, processed (compressed/decompressed) and put to the block write queue. From the write queue each processed block is picked up by the file writer thread and written to an output file.

### Sequence numbers
Each block of data is read from an input file and written to an output file sequentionally. To avoid messing with the blocks sequence and write all the processed blocks to an output file in the same order as they were read from an input file, there are two sequence numbers:
1. Read sequence nunmber - unique counter that starts from zero and assigns to each data block as it's read.
1. Write sequence number - unique counter that starts from zero and is incremented as a block of processed data with the same sequence number has been written to an output file.

For example: if the write sequence number is X and there's no processed block with the same sequence number in the file write queue, the file write thread will wait untill a block with the same sequence number is placed to the queue. After that the file write thread will pick up the block, write it to an output file and increment the write sequence number. And the loop will countinue until the last block of proccessed data will be written to an output file.


## Multihreading
Besides main thread that spawns by default from main() procedure the program spawns the following threads:
* Input file reader
* Output file writer
* Worker threads
* Worker threads dispatcher

#### A single input file reader thread
The thread sequentially reads blocks of data from a specified input file and places the blocks to the Block Processing Queue. Each block gets a unique sequential number which identifies the block in Block Processing and File Write queues and, also, during compression/decompression process.

The thread will pause reading the file if the File Write thread resets the Block Write Queue Ready signal in order to keep File Write Queue's lenght less than maximum allowed value.
The thread controls lenght of the Block Processing Queue. When the lenght of the queue exceeds predefined maximum the thread pauses read operations until a Block Processing thread picks up a block from the queue.

Depending of the selected operations mode the behaviour of the thread varies in the following fashion:
* Compression: reads each block with the size predefined in settings section and puts it to the processing queue as a byte array (Byte[]).
* Decompresison: for each block the thread reads its metadata (decompressed and compressed block size) that is stored in a compressed file, converts the metadata from a byte array to integer values, allocates a buffer required to read the compresissed block, and reads the block to the buffer. Finally the thread puts the compressed block with its metadata to the processing queue as a CGZipBlock object.

#### A single output file writer thread
The thread sequentially writes blocks of processed data from the Block Write Queue to the specified output file according to the block's sequence number.

The thread looks up a block with the subsequent write sequence number in the Block Write Queue. If there's no such block the thread waits until a Block Processing thread signals that a new block of data has been processed. After receiving such signal the thread do the look up once again until the block with the correct write sequence number is placed to the Block Write Queue by a Block Processing thread.

The thread controls lenght of the Block Write Queue. When the lenght of the queue exceeds predefined maximum the thread signals to the File Read Thread to pause read operations. After the lenght of the queue goes below the maxumum the thread signals to the File Read Thread to resume read operations.

Depending of the selected operations mode the behaviour of the thread varies in the following fashion:
* Compression: picks up a compressed data and its metadata as a CGZipBlock object from the Block Write Queue, converts it to a byte array and writes the array to the output file.
* Decompresison: picks up a decompressed data as a byte array (Byte[]) from the Block Write Queue and writes the array to the output file.

#### One or more worker threads.
An universal worker thread which either compress or decompress a block of data depending on which compression mode is selected.

The number of the threads depends on the number of CPU cores in a system which runs the program. Depending on the program's settings the number might be lowered if the corresponding settings is set to reserve one (or more) CPU core(s) for an operating system which runs the program.

Depending of the selected operations mode the behaviour of the thread varies in the following fashion:
* Compression: picks up an uncompressed data block as a byte array (Byte[]) from the Block Processing Queue, compresses it, transforms to CGZipBlock object, adds metadata to the object and puts it to the Block Write Queue.
* Decompresison: pick up a compressed CGZipBlock object from the Block Processing Queue, allocates a buffer for decompressed data according to the size that's stored in metadata, decompresses it and puts to the Block Write Queue.

#### A single worker threads dispatcher thread
Worker threads dispatcher thread that starts worker threads, cleans up finished thread, limits their number according to predefined settings and kills the threads in case of emergency shutdown.

#### Inter-thread communication
There are three signals that are used for communications betwen threads
* **A block of processed data has been written to an output file**. Fires when the file writer thread completes write operation for a data block. The signal is used to resume file read thread when it's suspended because the lenght of the processing queue has reached its maximum (defined by s_maxProcessingQueueLength variable)
* **The block write queue is ready to receive a new processed block**. The signal is used for throttling input file read thread when output file write queue becames longer than the limit which is specified in s_maxWriteQueueLength variable. It's manually set when the length of the block write queue is less than the allowed maximum and manually reset when the lenght of the block write queue is greather than the allowed maximum.
* **A block of data has been processed**. Fires when a block processing thread has finished processing a block of data. The signal is used to notify the File Write thread that a new block of data is ready, when the thread is suspended until a block of data with correct write sequence number appears in block write queue.
