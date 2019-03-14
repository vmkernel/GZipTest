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

### Queues
There are two queues:
1. Processing queue (stores block of input file until they are processed by a worker thread)
1. File write queue (stores the processed blocks until they are written to an output file)

Each queue is represented by a *Dictionary<Int32, Object>*.
Int32 is for a block's sequence number
Object is for a block of data. 

The final type of this Object varoab;e depends on the selected operation mode. 
If compression mode is selected then it's byte array (Byte[]) in processing queue and CGZipBlock object in file write queue.
If decompresion mode is selected then it's reversed: GZipBlock in processing queue and byte array (Byte[]) in file write queue.

The length of each queue is limited by s_maxProcessingQueueLength and s_maxWriteQueueLength variables in order to prevent RAM drain. When one of these limits is reached the file read thread suspends further read operations until both of the queues have at least one free slot for a data block.

Every item in the queues consists of the data block and its sequence number which is assigned to a block when it's read from an input file. The sequence number stays with the block of data until it's written to an output file. 
All blocks of an input file are read to the processing queue. From the processing queue each block is picked up by a block processing thread, processed (compressed/decompressed) and put to the block write queue. From the write queue each processed block is picked up by the file writer thread and written to an output file.

### Sequence numbers:
Each block of data is read from an input file and written to an output file sequentionally. To avoid messing with the blocks sequence and write all the processed blocks to an output file in the same order as they were read from an input file, there are two sequence numbers:
1. Read sequence nunmber - unique counter that starts from zero and assigns to each data block as it's read.
1. Write sequence number - unique counter that starts from zero and is incremented as a block of processed data with the same sequence number has been written to an output file.

For example: if the write sequence number is X and there's no processed block with the same sequence number in the file write queue, the file write thread will wait untill a block with the same sequence number is placed to the queue. After that the file write thread will pick up the block, write it to an output file and increment the write sequence number. And the loop will countinue until the last block of proccessed data will be written to an output file.



* An input uncompressed file is split to blocks with predefined size and these blocks are read sequentially by the input file read thread. After that each block is placed to the processing queue along with its sequential number.
* An input compressed file stores metadata that describes compressed block of data and the block itself. The metadata contains information about size in bytes of the compressed block (to be able to read the block from the input file) and information about its uncompressed size (to allocate decompressied data buffer preciesly).

This sequential number is used as a unique identifier for the block and after processing

## Multihreading
Besides main thread that spawns by default from main() procedure the program spawns the following threads:
#### A single input file read thread
#### A single output file write thread
#### A single worker threads dispatcher thread

#### One or more worker threads.
An universal worker thread which either compress or decompress a block of data depending on which compression mode is selected.

The number of the threads depends on the number of CPU cores in a system which runs the program. 
Depending on the program's settings the number might be lowered if the corresponding settings is set to reserve one (or more) CPU core(s) for an operating system which runs the program.

#### Inter-thread communication
There are three signals that are used for communications betwen threads

TBD
