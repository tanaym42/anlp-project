import zstandard
import io

with open('RC_2005-12.zst', 'rb') as fh:
    dctx = zstandard.ZstdDecompressor()
    stream_reader = dctx.stream_reader(fh)
    
    text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
    data = text_stream.read(100)
    print(data)