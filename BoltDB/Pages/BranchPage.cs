/* Copyright 2023 Hewlett Packard Enterprise Development LP.
 * 
 * You can redistribute this program and/or modify it under the terms of
 * the GNU Lesser Public License version 2.1
 */
using System.Collections.Generic;
using System.Text;
using Utilities;

namespace BoltDB
{
    internal class BranchPage : Page
    {
        public List<KeyValuePair<string, ulong>> Elements { get; set; }

        public BranchPage(byte[] page, ref int offset, int elementCount)
        {
            Elements = new List<KeyValuePair<string, ulong>>();
            for (int index = 0; index < elementCount; index++)
            {
                int currentOffset = offset;
                uint position = LittleEndianReader.ReadUInt32(page, ref offset);
                uint keySize = LittleEndianReader.ReadUInt32(page, ref offset);
                ulong pageID = LittleEndianReader.ReadUInt64(page, ref offset);
                
                byte[] keyBytes = ByteReader.ReadBytes(page, currentOffset + (int)position, (int)keySize);
                string key = Encoding.UTF8.GetString(keyBytes);
                Elements.Add(new KeyValuePair<string, ulong>(key, pageID));
            }
        }
    }
}
