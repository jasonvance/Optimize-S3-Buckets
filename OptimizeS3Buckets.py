#!/usr/bin/python2.7

__author__ = "Jason Vance"
__version__ = "0.1.2"
__date__ = "2014 04 14"


import os
import boto.s3.connection
import unicodedata

access_key = ''
secret_key = ''

LOCAL_PATH = '/tmp/'

conn = boto.connect_s3(
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    )

bucket = conn.get_bucket('marketplace-images-production')


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def RecursiveOptimizeBucket():

    total_savings = 0
    count = 0

    for key in bucket.list():
        try:
            #Put this in because previous developers didn't sanitize file names
            unicodedata.normalize('NFKD', key.name).encode('ascii', 'ignore')

            print 'Fetching: ' + key.name

            print u"{name}\t{size}\t{modified}".format(
                name=key.name,
                size=key.size,
                modified=key.last_modified,
                encoding='utf-8'
            )

            size_orig = key.size

            bucketFile = bucket.get_key(key.name)

            keystring = str(key.name)

            directoryname = keystring.strip(keystring.split('/')[-1])

            filename = keystring.split('/')[-1]

            gettype = filename.split('.')[-1]


            bucketdir = LOCAL_PATH + directoryname

#           make dir if there is not one available

            if not os.path.exists(bucketdir):
                try:
                    os.makedirs(bucketdir)
                except OSError as exception:
                    if exception.errno != errno.EEXIST:
                        raise


            fn = LOCAL_PATH + keystring

            if gettype == 'jpg':

                try:
                    key.get_contents_to_filename(fn)

                except OSError, e:

                    print e

                jpegcommand = 'jpegoptim ' + fn

                os.system(jpegcommand)

                jpeg_size = os.path.getsize(fn)

                size_savings = key.size - jpeg_size

                if size_savings > 0:

                    UploadToAWS(fn, keystring, size_savings, opt_size, count, total_savings, size_orig)

                else:

                    os.remove(fn)

            elif gettype == 'png':

                try:
                    key.get_contents_to_filename(fn)

                except OSError, e:
                    print e

                fcommand = 'pngquant  --quality=65-80 --speed 1 -f ' + LOCAL_PATH + keystring + ' --output ' + fn

                optcommand = 'optipng ' + fn

                os.system(fcommand)

                pngquant_size = os.path.getsize(fn)

                os.system(optcommand)

                opt_size = os.path.getsize(fn)

                size_savings = key.size - opt_size

                if size_savings > 0:

                    UploadToAWS(fn, keystring, size_savings, opt_size, count, total_savings, size_orig)

                else:
                    os.remove(fn)

            else:

                print 'No Savings Skipping:' + keystring

        except  Exception as inst:
                print type(inst)     # the exception instance
                print inst.args      # arguments stored in .args
                print inst           # __str__ allows args to be printed directly

def UploadToAWS(fn, keystring, size_savings, opt_size, count, total_savings, size_orig):
     # If the file is smaller, put it back on S3
            try:
                #aws s3 cp s3://marketplace-images-production s3://marketplace-images-production-bak --recursive
                #just a hack so don't judge
                #Need to fix to use boto
                aws_command = 'aws s3 cp ' + fn + ' s3://marketplace-images-production/' + keystring
                os.system(aws_command)

                total_savings = total_savings + size_savings

                saving_human_readable = sizeof_fmt(total_savings)

                os.remove(fn)

                count = count + 1


                print 'Images Processed: ' + str(count)

                print saving_human_readable

                return count, total_savings

            except  Exception as inst:
                print type(inst)     # the exception instance
                print inst.args      # arguments stored in .args
                print inst           # __str__ allows args to be printed directly

if __name__ == '__main__':
    RecursiveOptimizeBucket()