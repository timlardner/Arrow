import hashlib

import pyarrow as pa
import pyarrow.plasma as plasma

from common.utils import format_size


class DataHandler:
    def __init__(self):
        self.client = plasma.connect("/tmp/plasma", "", 0)
        
    @staticmethod
    def get_cache_key(string):
        return hashlib.sha1(string.encode()).digest()

    def check_for_object(self, key):
        object_key = self.get_cache_key(key)
        object_id = plasma.ObjectID(object_key)
        return self.client.contains(object_id)


class DataWriter(DataHandler):
    def __init__(self):
        super().__init__()

    def cache_data_frame(self, df, key, force_eviction=False):
        object_key = self.get_cache_key(key)
        object_id = plasma.ObjectID(object_key)
        if self.client.contains(object_id):
            string = 'DataWriter: Object exists in cache'
            if force_eviction:
                print('{} - evicting'.format(string))
                self.client.release(object_id)
            else:
                raise Exception(string)

        record_batch = pa.RecordBatch.from_pandas(df)

        # Work out how large our data frame is
        mock_sink = pa.MockOutputStream()
        stream_writer = pa.RecordBatchStreamWriter(mock_sink, record_batch.schema)
        stream_writer.write_batch(record_batch)
        stream_writer.close()
        data_size = mock_sink.size()
        print('DataWriter: Data size is {}'.format(format_size(data_size)))

        # Actually write the data frame to the cache
        buf = self.client.create(object_id, data_size)
        stream = pa.FixedSizeBufferWriter(buf)
        stream_writer = pa.RecordBatchStreamWriter(stream, record_batch.schema)
        stream_writer.write_batch(record_batch)
        stream_writer.close()

        # Make item available to other processes
        self.client.seal(object_id)


class DataReader(DataHandler):
    def __init__(self):
        super().__init__()

    def load_data_frame(self, key):
        object_key = self.get_cache_key(key)
        object_id = plasma.ObjectID(object_key)
        if not self.client.contains(object_id):
            return None

        [data] = self.client.get_buffers([object_id])
        buffer = pa.BufferReader(data)
        reader = pa.RecordBatchStreamReader(buffer)
        record_batch = reader.read_next_batch()
        df = record_batch.to_pandas()
        return df
