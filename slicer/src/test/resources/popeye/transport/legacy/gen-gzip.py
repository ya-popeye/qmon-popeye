import zlib
content = ""
for str in range(1, 10):
  content += "I'm Bart Simpson\n"
zcontent = zlib.compress(content)
print zcontent
