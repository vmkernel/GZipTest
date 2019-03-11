using System;

namespace GZipTest
{
    // Represents a block of data compressed using GZipStream class 
    // with the data itself, and metadata (uncompressed and compressed size) which is stored in an ouput file along with the data.
    public class CGZipBlock
    {
        #region FIELDS
        #region Metadata
        // Buffer size for uncompressed data block size info
        // Uses to allocate uncompressed data block size buffer and corresponding read operations for the buffer
        private static Int32 MetadataUncompressedBlockSizeBufferLength
        {
            get
            {
                return sizeof(Int32);
            }
        }

        // Buffer size for compressed data block size info
        // Uses to allocate compressed data block size buffer and corresponding read operations for the buffer
        private static Int32 MetadataCompressedBlockSizeBufferLength
        {
            get
            {
                return sizeof(Int32);
            }
        }

        // Buffer size for all metadate
        // Uses to allocate metadata buffer and corresponding read operations for the buffer
        public static Int32 MetadataSize
        {
            get
            {
                return MetadataCompressedBlockSizeBufferLength + MetadataUncompressedBlockSizeBufferLength;
            }
        }
        #endregion

        // Size (in bytes) of an original (uncompressed) block of data
        public Int32 DataSizeUncompressed { get; set; }

        // Size (in bytes) of a compressed block of data
        public Int32 DataSizeCompressed
        {
            get
            {
                // Gets from allocated data buffer
                return Data.Length;
            }
        }

        // Block of GZip-compressed data without any metadata
        private Byte[] s_data;
        public Byte[] Data
        {
            // In order to prevent NullReferrenceException, gets and sets an empty array instead of null
            get
            {
                if (s_data == null)
                {
                    s_data = new Byte[0];
                }
                return s_data;
            }

            set
            {
                if (value == null)
                {
                    s_data = new Byte[0];
                }
                s_data = value;
            }
        }
        #endregion

        #region FUNCTIONS AND METHODS
        // Converts compressed data and its metadata to byte array (which might be written to an output file, for example)
        public Byte[] ToByteArray()
        {
            try
            {
                // Converting uncompressed and compressed size from integer value to byte array
                Byte[] uncompressedSizeBuffer = BitConverter.GetBytes(DataSizeUncompressed);
                Byte[] compressedSizeBuffer = BitConverter.GetBytes(DataSizeCompressed);

                if ((uncompressedSizeBuffer.Length + compressedSizeBuffer.Length) != CGZipBlock.MetadataSize)
                {
                    throw new Exception("Expected and calculated metadata block size mismatch.");
                }

                // Allocating resultant byte array
                Int32 resultantBufferLength = uncompressedSizeBuffer.Length + compressedSizeBuffer.Length + Data.Length;
                Byte[] resultantBuffer = new Byte[resultantBufferLength];

                // Copying metadata and comressed data to the resultant array
                Array.Copy(uncompressedSizeBuffer, 0, resultantBuffer, 0, uncompressedSizeBuffer.Length);
                Array.Copy(compressedSizeBuffer, 0, resultantBuffer, uncompressedSizeBuffer.Length, compressedSizeBuffer.Length);
                Array.Copy(Data, 0, resultantBuffer, uncompressedSizeBuffer.Length + compressedSizeBuffer.Length, Data.Length);

                return resultantBuffer;
            }
            catch (Exception ex)
            {
                throw new Exception("An unhandled exception has occured while attempting to convert a GZip block object to a byte array", ex);
            }
        }

        // Initialize compressed data buffer and metedata from byte buffer that represents the metadata (which has been read from a compressed file, for example)
        public void InitializeWithMetadata(Byte[] metadataBuffer)
        {
            try
            {
                // Allocating buffers for comversion from byte array to integer value
                Byte[] uncompressedBuffer = new Byte[MetadataUncompressedBlockSizeBufferLength];
                Byte[] compressedBuffer = new Byte[MetadataCompressedBlockSizeBufferLength];

                // Splitting metadata buffer in pieces of uncompressed and compressed block sizes
                Array.Copy(metadataBuffer, 0, uncompressedBuffer, 0, MetadataUncompressedBlockSizeBufferLength);
                Array.Copy(metadataBuffer, MetadataUncompressedBlockSizeBufferLength, compressedBuffer, 0, MetadataCompressedBlockSizeBufferLength);

                // Converting bytes representations of the sizes to integer
                DataSizeUncompressed = BitConverter.ToInt32(uncompressedBuffer, 0); // Write directly to the uncompressed data block size variable 
                Int32 dataSizeCompressed = BitConverter.ToInt32(compressedBuffer, 0);

                // Allocating compressed data buffer according to compressed data size
                // Later DataSizeCompressed property will calculate compressed block size accroding to the data buffer's size
                s_data = new Byte[dataSizeCompressed];
            }
            catch (Exception ex)
            {
                throw new Exception("An unhandled exception has occured while attempting to initialize a GZip block object from a metadata buffer", ex);
            }
        }
        #endregion
    }
}
