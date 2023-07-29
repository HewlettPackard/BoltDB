/* Copyright 2023 Hewlett Packard Enterprise Development LP.
 * 
 * You can redistribute this program and/or modify it under the terms of
 * the GNU Lesser Public License version 2.1
 */
using System;
using System.Collections.Generic;
using System.IO;
using Utilities;

namespace BoltDB
{
    public class BoltDatabase
    {
        private const int DefaultPageSize = 4096;

        private Stream m_stream;
        private int m_pageSize;
        private MetaPage m_metaPage1;
        private MetaPage m_metaPage2;

        public BoltDatabase(string path)
        {
            m_stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            ReadMetaPages();
        }

        public BoltDatabase(Stream stream)
        {
            m_stream = stream;
            ReadMetaPages();
        }

        private void ReadMetaPages()
        {
            m_pageSize = DefaultPageSize;
            byte[] page1 = ByteReader.ReadBytes(m_stream, m_pageSize);
            byte[] page2;
            int offset = 0;
            PageHeader page1Header = new PageHeader(page1, ref offset);
            PageHeader page2Header;
            if (page1Header.IsMetaPage)
            {
                m_metaPage1 = new MetaPage(page1, ref offset);
            }

            if (page1Header.IsMetaPage && m_metaPage1.IsValid())
            {
                m_pageSize = (int)m_metaPage1.PageSize;
                if (m_pageSize != DefaultPageSize)
                {
                    m_stream.Seek(m_pageSize, SeekOrigin.Begin);
                }
            }

            page2 = ByteReader.ReadBytes(m_stream, m_pageSize);
            offset = 0;
            page2Header = new PageHeader(page1, ref offset);
            if (page2Header.IsMetaPage)
            {
                m_metaPage2 = new MetaPage(page2, ref offset);
            }

            bool isFirstPageValid = page1Header.IsMetaPage && m_metaPage1.IsValid();
            bool isSecondPageValid = page2Header.IsMetaPage && m_metaPage2.IsValid();
            if (!isFirstPageValid && !isSecondPageValid)
            {
                throw new InvalidDataException("BoltDB database is invalid, first and second pages are not valid");
            }
        }

        private MetaPage GetCurrentMetaPage()
        {
            bool isFirstPageValid = m_metaPage1 != null && m_metaPage1.IsValid();
            bool isSecondPageValid = m_metaPage2 != null && m_metaPage2.IsValid();
            if (isFirstPageValid && isSecondPageValid)
            {
                if (m_metaPage1.TxID > m_metaPage2.TxID)
                {
                    return m_metaPage1;
                }
                else
                {
                    return m_metaPage2;
                }
            }
            else if (isFirstPageValid)
            {
                return m_metaPage1;
            }
            else
            {
                return m_metaPage2;
            }
        }

        private Page ReadPage(ulong pageID)
        {
            long offsetInFile = (long)pageID * m_pageSize;
            m_stream.Seek(offsetInFile, SeekOrigin.Begin);
            byte[] pageBytes = ByteReader.ReadBytes(m_stream, m_pageSize);
            int offset = 0;
            PageHeader pageHeader = new PageHeader(pageBytes, ref offset);
            if (pageHeader.Overflow > 0)
            {
                byte[] overflowPages = ByteReader.ReadBytes(m_stream, m_pageSize * (int)pageHeader.Overflow);
                pageBytes = ByteUtils.Concatenate(pageBytes, overflowPages);
            }

            if (pageHeader.IsBranchPage)
            {
                return new BranchPage(pageBytes, ref offset, pageHeader.Count);
            }
            else if (pageHeader.IsLeafPage)
            {
                return new LeafPage(pageBytes, ref offset, pageHeader.Count);
            }
            else if (pageHeader.IsFreeListPage)
            {
                return null;
            }
            else
            {
                throw new InvalidDataException("Invalid page");
            }
        }

        public Bucket GetRootBucket()
        {
            MetaPage metaPage = GetCurrentMetaPage();
            return GetBucket(metaPage.Root);
        }

        public Bucket GetBucket(string key)
        {
            MetaPage metaPage = GetCurrentMetaPage();
            Bucket rootBucket = GetBucket(metaPage.Root);
            return rootBucket.GetElementValueByKey(key) as Bucket;
        }

        internal Bucket GetBucket(BucketLocation bucketLocation)
        {
            if (bucketLocation.IsInlineBucket)
            {
                throw new InvalidOperationException();
            }

            return new Bucket(this, bucketLocation);
        }

        internal List<KeyValuePair<string, object>> ReadBucket(BucketLocation bucketLocation)
        {
            if (bucketLocation.IsInlineBucket)
            {
                throw new InvalidOperationException();
            }

            ulong pageID = bucketLocation.Root;
            List<LeafPage> pages = ReadAllLeafPages(pageID);
            List<KeyValuePair<string, object>> result = new List<KeyValuePair<string, object>>();
            foreach(LeafPage leaf in pages)
            {
                List<KeyValuePair<string, object>> leafElements = new List<KeyValuePair<string, object>>(leaf.Elements);
                // Replace bucket locations with Bucket instances
                for (int index = 0; index < leafElements.Count; index++)
                {
                    if (leafElements[index].Value is BucketLocation valueBucketLocation)
                    {
                        leafElements[index] = new KeyValuePair<string, object>(leafElements[index].Key, new Bucket(this, valueBucketLocation));
                    }
                }
                result.AddRange(leafElements);
            }

            return result;
        }

        internal List<LeafPage> ReadAllLeafPages(ulong pageID)
        {
            Page page = ReadPage(pageID);
            List<LeafPage> result = new List<LeafPage>();
            if (page is LeafPage leaf)
            {
                result.Add(leaf);
                return result;
            }
            else if (page is BranchPage branch)
            {
                for (int index = 0; index < branch.Elements.Count; index++)
                {
                    ulong elementPageID = branch.Elements[index].Value;
                    List<LeafPage> elementPages = ReadAllLeafPages(elementPageID);
                    result.AddRange(elementPages);
                }
                return result;
            }
            else
            {
                throw new InvalidDataException("Invalid page type");
            }
        }

        //public List<KeyValuePair<string, object>> ReadRootElements()
        //{
        //    MetaPage metaPage = GetCurrentMetaPage();
        //    return ReadBucket(metaPage.Root);
        //}
    }
}