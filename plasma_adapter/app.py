import hashlib

import pyarrow as pa
import pyarrow.plasma as plasma


class DataHandler:
    def __init__(self):
        self.client = plasma.connect("/tmp/plasma", "", 0)
        
    @staticmethod
    def get_cache_key(string):
        return hashlib.sha1(string.encode()).digest()


class DataWriter(DataHandler):
    def __init__(self):
        super().__init__()

    def cache_data_frame(self, df, key):
        record_batch = pa.RecordBatch.from_pandas(df)
        object_key = self.get_cache_key(key)
        object_id = plasma.ObjectID(object_key)

        # Work out how large our data frame is
        mock_sink = pa.MockOutputStream()
        stream_writer = pa.RecordBatchStreamWriter(mock_sink, record_batch.schema)
        stream_writer.write_batch(record_batch)
        stream_writer.close()
        data_size = mock_sink.size()
        print('DataWriter: Data size is {} bytes'.format(str(data_size)))

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

        [data] = self.client.get_buffers([object_id])
        buffer = pa.BufferReader(data)
        reader = pa.RecordBatchStreamReader(buffer)
        record_batch = reader.read_next_batch()
        df = record_batch.to_pandas()
        return df