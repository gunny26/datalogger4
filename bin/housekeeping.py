#!/usr/bin/python3
import sys
import os
import datetime
import shutil
import logging
import argparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s : %(message)s")

def main(archive_dir, data_dir, back):
    def inner(directory, oldest):
        for filename in sorted(os.listdir(directory)):
            if len(filename) != 10 or len(filename.split("-")) != 3:
                logging.error("skipping non datestring directory %s", filename)
                continue
            if filename < oldest:
                absfilename = os.path.join(directory, filename)
                if os.path.isdir(absfilename):
                    assert absfilename != directory
                    if not args.force:
                        logging.info("would delete subdirectory %s, use -f to do it", absfilename)
                    else:
                        logging.info("deleting subdirectory %s", absfilename)
                        shutil.rmtree(absfilename)
    now = datetime.date.today()
    oldest = now - datetime.timedelta(days=back)
    oldest = oldest.isoformat()
    logging.info("deleting analyzed data older than %s", oldest)
    inner(data_dir, oldest)
    oldest = now - datetime.timedelta(days=back * 2)
    oldest = oldest.isoformat()
    logging.info("deleting archived raw input data older than %s", oldest)
    inner(archive_dir, oldest)


if __name__ == "__main__":
    archive_dir = "/srv/raw-archiv/datalogger_raw_archiv/"
    data_dir = "/srv/data"
    parser = argparse.ArgumentParser(description='deleting old data')
    parser.add_argument('--archive-dir', default=archive_dir, help="basedirectory of archived raw_data: %(default)s")
    parser.add_argument('--data-dir', default=data_dir, help="basedirectory of archived raw_data: %(default)s")
    parser.add_argument("-b", '--back', default=400, type=int, help="online data older than --back days will be purged, and archived raw data older than 2 * this value")
    parser.add_argument("-f", '--force', action='store_true', help="force the deletion, otherwise only show what will be done")
    parser.add_argument("-q", '--quiet', action='store_true', help="set to loglevel ERROR")
    parser.add_argument("-v", '--verbose', action='store_true', help="set to loglevel DEBUG")
    args = parser.parse_args()
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    logging.debug(args)
    if not args.force:
        logging.error("-f is not set, so only displaying what would be done")
    main(args.archive_dir, args.data_dir, args.back)
