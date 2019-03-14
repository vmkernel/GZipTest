using System;

namespace GZipTest
{
    /// <summary>
    /// Represents a block of data that is compressed using GZipStream class with and metadata.
    /// The metadata contains original size in bytes of the uncompressed data and size in bytes of the compressed data along with the data itself.
    /// </summary>
    public class CGZipBlock
    {
        #region FIELDS
        #region Metadata
        #region Metadata buffers
        /// <summary>
        /// Size of a buffer that is used to store byte-encoded representation of integer size of uncompressed data block.
        /// Is used to allocate the corresponding buffer and write to it.
        /// </summary>
        private static Int32 MetadataUncompressedBlockSizeBufferLength
        {
            get
            {
                return sizeof(Int32);
            }
        }

        /// <summary>
        /// Size of a buffer that is used to store byte-encoded representation of integer size of compressed data block.
        /// Is used to allocate the corresponding buffer and write to it.
        /// </summary>
        private static Int32 MetadataCompressedBlockSizeBufferLength
        {
            get
            {
                return sizeof(Int32);
            }
        }
        #endregion

        #region Sizes
        /// <summary>
        /// Size of a buffer that is required to store all metadate of the GZip-block
        /// Is used to allocate the corresponding buffer and write to it.
        /// </summary>
        public static Int32 MetadataSize
        {
            get
            {
                return MetadataCompressedBlockSizeBufferLength + MetadataUncompressedBlockSizeBufferLength;
            }
        }
        
        /// <summary>
        /// Size in bytes of an original uncompressed block of data.
        /// </summary>
        public Int32 DataSizeUncompressed { get; set; }

        /// <summary>
        /// Size in bytes of a compressed block of data.
        /// Evaluates dynamically, based on compressed data buffer size.
        /// </summary>
        public Int32 DataSizeCompressed
        {
            get
            {
                // Gets from allocated data buffer
                return Data.Length;
            }
        }
        #endregion
        #endregion

        #region Compressed data block
        /// <summary>
        /// Block of GZip-compressed data with no metadata
        /// </summary>
        private Byte[] s_data;
        /// <summary>
        /// Block of GZip-compressed data with no metadata
        /// </summary>
        public Byte[] Data
        {
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
        #endregion

        #region FUNCTIONS AND METHODS
        /// <summary>
        /// Converts compressed data block and its metadata to byte array (which might be written to an output file, for example)
        /// The order is: Original block size, Compressed block size, Compressed data
        /// </summary>
        /// <returns>A byte array that represents metadata and compressed data itself</returns>
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

        /// <summary>
        /// Initialize compressed data buffer and metedata from byte array that represents the metadata of a compressed block(which has been read from a compressed file, for example)
        /// </summary>
        /// <param name="metadataBuffer">Byte array that represents the metadata of the compressed block</param>
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
