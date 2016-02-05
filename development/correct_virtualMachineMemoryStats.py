#!//usr/bin/python

import glob
import gzip
import os


fieldnames_new = ("ts", "hostname", "instance", "mem.active.average", "mem.consumed.average", "mem.granted.average", "mem.overhead.average", "mem.shared.average", "mem.swapped.average")
fieldnames_old = ("ts", "hostname", "mem.active.average", "mem.consumed.average", "mem.granted.average", "mem.overhead.average", "mem.shared.average", "mem.swapped.average")

for filename in sorted(glob.glob("/var/rrd/vicenter/raw/virtualMachineMemoryStats*")):
    if "2016-01-18" not in filename:
        print "skipping corrected data"
        continue
    print "reading file %s" % os.path.basename(filename)
    fh = None
    if filename.endswith(".gz"):
        fh = gzip.open(filename, "r")
    else:
        fh = open(filename, "r")
    data = fh.read()
    fh.close()
    print "correcting data"
    outbuffer = []
    outbuffer.append("\t".join(fieldnames_new))
    for line in data.split("\n")[1:]:
        if line == "":
            continue
        outline = None
        fields = line.split("\t")
        if len(fields) == len(fieldnames_old):
            outline = fields[:2] + ["all",] + fields[2:]
        elif len(fields) == len(fieldnames_new):
            outline = fields
        else:
            print "corrupt line found, %d fields" % len(fields)
            print fields
            continue
        assert len(outline) == len(fieldnames_new)
        outbuffer.append("\t".join(outline))
    outfilename = os.path.join("/var/rrd/tmp", os.path.basename(filename))
    if not outfilename.endswith(".gz"):
        outfilename += ".gz"
    print "writing %d lines to new file %s" % (len(outbuffer), outfilename)
    outfile = gzip.open(outfilename, "w")
    outfile.write("\n".join(outbuffer))
    outfile.close()
