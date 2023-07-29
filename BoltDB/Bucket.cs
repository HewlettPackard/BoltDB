/* Copyright 2023 Hewlett Packard Enterprise Development LP.
 * 
 * You can redistribute this program and/or modify it under the terms of
 * the GNU Lesser Public License version 2.1
 */
using System;
using System.Collections.Generic;
using System.Text;

namespace BoltDB
{
    public class Bucket
    {
        private BoltDatabase m_boltDatabase;
        private BucketLocation m_bucketLocation;
        private List<KeyValuePair<string, object>> m_inlineElements;

        internal Bucket(BoltDatabase boltDatabase, BucketLocation bucketLocation)
        {
            m_boltDatabase = boltDatabase;
            m_bucketLocation = bucketLocation;
        }

        internal Bucket(BoltDatabase boltDatabase, List<KeyValuePair<string, object>> elements)
        {
            m_boltDatabase = boltDatabase;
            m_inlineElements = elements;
        }

        public object GetElementValueByKey(string key)
        {
            List<KeyValuePair<string, object>> elements = GetElements();
            int index = Utilities.SortedList<KeyValuePair<string, object>>.FirstIndexOf(elements, CompareKeys, new KeyValuePair<string, object>(key, null));
            if (index == -1)
            {
                return null;
            }

            return elements[index].Value;
        }

        public string GetStringValueByKey(string key)
        {
            byte[] value = GetElementValueByKey(key) as byte[];
            if (value != null)
            {
                return Encoding.UTF8.GetString(value);
            }
            return null;
        }

        public Bucket GetBucket(string key)
        {
            return GetElementValueByKey(key) as Bucket;
        }

        public int CompareKeys(KeyValuePair<string, object> a, KeyValuePair<string, object> b)
        {
            return string.Compare(a.Key, b.Key, StringComparison.Ordinal);
        }

        public List<KeyValuePair<string, object>> GetElements()
        {
            if (m_inlineElements != null)
            {
                return m_inlineElements;
            }

            return m_boltDatabase.ReadBucket(m_bucketLocation);
        }

        public List<KeyValuePair<string, Bucket>> GetBuckets()
        {
            List<KeyValuePair<string, Bucket>> result = new List<KeyValuePair<string, Bucket>>();
            List<KeyValuePair<string, object>> elements = GetElements();
            foreach (KeyValuePair<string, object> element in elements)
            {
                if (element.Value is Bucket bucket)
                {
                    result.Add(new KeyValuePair<string, Bucket>(element.Key, bucket));
                }
            }

            return result;
        }
    }
}
