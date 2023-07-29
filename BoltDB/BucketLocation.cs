/* Copyright 2023 Hewlett Packard Enterprise Development LP.
 * 
 * You can redistribute this program and/or modify it under the terms of
 * the GNU Lesser Public License version 2.1
 */
using System.IO;
using Utilities;

namespace BoltDB
{
    /// <remarks>
    /// bucket represents the on-file representation of a bucket.
    /// This is stored as the "value" of a bucket key. If the bucket is small enough,
    /// then its root page can be stored inline in the "value", after the bucket
    /// header. In the case of inline buckets, the "root" will be 0.
    /// </remarks>
    internal class BucketLocation
    {
        /// <remarks>page id of the bucket's root-level page</remarks>
        public ulong Root { get; private set; }

        /// <remarks>monotonically incrementing, used by NextSequence()</remarks>
        public ulong Sequence { get; private set; }

        public BucketLocation(ulong root, ulong sequence)
        {
            Root = root;
            Sequence = sequence;
        }

        public void Write(byte[] buffer, ref int offset)
        {
            LittleEndianWriter.WriteUInt64(buffer, ref offset, Root);
            LittleEndianWriter.WriteUInt64(buffer, ref offset, Sequence);
        }

        public void Write(Stream stream)
        {
            LittleEndianWriter.WriteUInt64(stream, Root);
            LittleEndianWriter.WriteUInt64(stream, Sequence);
        }

        public bool IsInlineBucket
        {
            get
            {
                return Root == 0;
            }
        }

        public static BucketLocation ReadBucketLocation(byte[] page, ref int offset)
        {
            ulong root = LittleEndianReader.ReadUInt64(page, ref offset);
            ulong sequence = LittleEndianReader.ReadUInt64(page, ref offset);
            return new BucketLocation(root, sequence);
        }
    }
}
