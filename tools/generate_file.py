import sys
import os

args = sys.argv

"""
    How to use

    python generate_file.py -size=1MB -pattern=this_is_my_data -width=100 [-num=true]

    -size
        The result file will be exactly this size, the last record may be truncated

    -patten
        Specify data to be added to the available space on the line
        Patterns that don't take up all avalable space will be left padded with ' '
        Patterns larger than the available space will be truncated to fit

    -width of a line, in bytes, including the training '\n' delimiter

    -num (optional)
        Prepend a number to each line, the number will be 16 chars wide padded with ' ' on the left
        Takes 16 chars out of the space available for pattern
"""



#------------------------------------------------------------------------------
#   Argument Parsing
#------------------------------------------------------------------------------

if len(args) == 1:
  print "\nusage: generate_file.py -size=[X]MB -pattern=this_is_my_pattern -width=100 -num=true -o=test.txt\n"
  sys.exit(1)

opts = []
for idx,item in enumerate(args):
    if idx == 0:
        continue
    opts.append(item)

file_size = 0
file_pattern = ""
record_width = 0
file_num = False

output_file = None

for opt in opts:
    key, val = opt.split('=')
    key = key[1:]

    if key == "size":
        print "parsing size"
        mult = val[-2:]
        coef = val[:-2]
        
        if mult != "MB":
            print "only megabyte multipliers supported"
            sys.exit(1)
        file_size = int(coef) * 1024 * 1024

    elif key == "pattern":
        print "parsing pattern"
        file_pattern = val

    elif key == "width":
        print "parsing width"
        record_width = int(val)

    elif key == "num":
        print "parsing num"
        if val == "true":
            file_num = True
        else:
            file_num = False

    elif key == "o":
        print "parsing output file"
        output_file = val

    else:
        print "Option not recognized"
        print opt
        sys.exit(1)


# make the line buffer, should get this out of global namespace
avail_content_width = record_width - len('\n')
if file_num == True:
    avail_content_width -= 16

if(avail_content_width <= 0):
    print("lines not wide enough to store any data, increase width")

print avail_content_width

line_buffer = None

if len(file_pattern) < avail_content_width:
    padding = '_' * ( avail_content_width - len(file_pattern) )
    line_buffer = file_pattern + padding
elif len(paddten > avail_content_width):
    line_buffer = file_pattern[:avail_content_width]
else:
    line_buffer = file_pattern




def make_padded_integer(val):
    assert type(val) == int

    val_str = str(val)
    val_len = len(val_str)

    # 10E15 sould be sufficient
    assert val_len <= 16

    pad_len = 16 - val_len

    assert pad_len >= 0

    pad_str = ' '*pad_len

    return pad_str + val_str



x = make_padded_integer(15)

print "'" + x + "'"
print "file size = " + str(file_size)

f = open(output_file, 'w')
written_bytes = 0
record_count = 0
while written_bytes + record_width <= file_size:

    line = ""
    if file_num:
        line += make_padded_integer(record_count)
    line += line_buffer + "\n"


    f.write(line)

    written_bytes += record_width
    record_count += 1

if written_bytes < file_size:
    diff = file_size - written_bytes
    assert diff > 0
    assert diff < record_width

    line = ""
    if file_num:
        line += make_padded_integer(record_count)
    line += line_buffer + '\n'

    line = line[:diff]
    f.write(line)

f.close()


