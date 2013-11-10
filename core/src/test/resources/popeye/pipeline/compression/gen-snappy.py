import snappy
import StringIO

content = ""
for str in range(1, 10):
    content += "I'm Bart Simpson\n"
scontent = StringIO.StringIO(content)
output = StringIO.StringIO()
snappy.stream_compress(scontent, output)
print output.getvalue()
