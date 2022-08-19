import time

import pyarrow as pa
import pyarrow.flight as pf
import pandas as pd


while True:

    try:

        # https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightClient.html?highlight=flightclient#pyarrow.flight.FlightClient
        connection: pf.FlightClient = pf.connect('grpc://0.0.0.0:38709')

        keys = pa.array([1000, 2000], type=pa.uint64())
        strings: pa.StringArray = pa.array(['some', 'text'])
        descriptor = pf.FlightDescriptor.for_command('write')
        data = pa.record_batch([keys, strings], names=['keys', 'vals'])
        options = pf.FlightCallOptions()
        handle = connection.do_put(descriptor, data.schema, options)
        writer: pf.FlightStreamWriter = handle[0]
        reader: pf.FlightMetadataReader = handle[1]
        writer.write_batch(data)
        writer.done_writing()

        descriptor = pf.FlightDescriptor.for_command('read')
        data = pa.record_batch([keys], names=['keys'])
        handle = connection.do_exchange(descriptor, options)
        writer: pf.FlightStreamWriter = handle[0]
        reader: pf.FlightStreamReader = handle[1]
        writer.write_batch(data)
        writer.done_writing()
        result = reader.read_pandas()
        print(result)

    except Exception as e:
        print('Received error:', e)
        time.sleep(1)
