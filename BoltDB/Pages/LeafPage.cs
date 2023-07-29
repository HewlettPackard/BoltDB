/* Copyright 2023 Hewlett Packard Enterprise Development LP.
 * 
 * You can redistribute this program and/or modify it under the terms of
 * the GNU Lesser Public License version 2.1
 */
using System.Collections.Generic;
using System.IO;
using System.Text;
using Utilities;

namespace BoltDB
{
    internal class LeafPage : Page
    {
        public List<KeyValuePair<string, object>> Elements { get; set; }

        public LeafPage(byte[] page, ref int offset, int elementCount)
        {
            Elements = new List<KeyValuePair<string, object>>();
            for(int index = 0; index < elementCount; index++)
            {
                int currentOffset = offset;
                LeafPageElementFlags flags = (LeafPageElementFlags)LittleEndianReader.ReadUInt32(page, ref offset);
                uint position = LittleEndianReader.ReadUInt32(page, ref offset);
                uint keySize = LittleEndianReader.ReadUInt32(page, ref offset);
                uint valueSize = LittleEndianReader.ReadUInt32(page, ref offset);

                bool isBucket = (flags & LeafPageElementFlags.Bucket) > 0;
                byte[] keyBytes = ByteReader.ReadBytes(page, currentOffset + (int)position, (int)keySize);
                byte[] valueBytes = ByteReader.ReadBytes(page, currentOffset + (int)position + (int)keySize, (int)valueSize);

                object value;
                if (isBucket)
                {
                    int offsetInValue = 0;
                    BucketLocation bucketLocation = BucketLocation.ReadBucketLocation(valueBytes, ref offsetInValue);
                    if (bucketLocation.IsInlineBucket)
                    {
                        value = ReadInlineBucket(valueBytes, offsetInValue);
                    }
                    else
                    {
                        value = bucketLocation;
                    }
                }
                else
                {
                    value = valueBytes;
                }
                string key = Encoding.UTF8.GetString(keyBytes);
                Elements.Add(new KeyValuePair<string, object>(key, value));
            }
        }

        private Bucket ReadInlineBucket(byte[] bucketBytes, int offset)
        {
            PageHeader header = new PageHeader(bucketBytes, ref offset);
            if (header.IsLeafPage)
            {
                LeafPage leaf = new LeafPage(bucketBytes, ref offset, header.Count);
                return new Bucket(null, leaf.Elements);
            }
            else
            {
                // Only leaf pages are inlineable
                throw new InvalidDataException("Invalid inline page type");
            }
        }
    }
}
