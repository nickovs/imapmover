#!/usr/bin/env python
# Copyright 2021 Nicko van Someren
#
# Licensed under the Apache License, Version 2.0 (the "License")
# See the LICENSE.txt file for details

# SPDX-License-Identifier: Apache-2.0

"""Provide a command line interface for imapmover"""

import argparse
import fnmatch
import functools
from getpass import getpass

from imapmover import imap_sync, ServerInfo
from tqdm import tqdm


def _folder_matcher(pattern_list, folder_list):
    # A function to match folder names against an ordered list of
    # inclusions and exclusions

    # If the list is non-empty and starts with an include then we start with
    # an empty list, otherwise we start with the full list.
    if pattern_list and pattern_list[0][0] == "+":
        result = []
    else:
        result = folder_list

    for direction, pattern in pattern_list:
        if direction == '+':
            include = fnmatch.filter(folder_list, pattern)
            result.extend(name for name in include if name not in result)
        else:
            exclude = fnmatch.filter(result, pattern)
            result = [name for name in result if name not in exclude]

    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-host",
                        help="hostname of source IMAP server",
                        default="localhost", metavar="HOSTNAME")
    parser.add_argument("--src-port",
                        help="port of source IMAP server",
                        default=None, metavar="PORT")
    parser.add_argument("--src-no-ssl",
                        help="connect without SSL/TLS on source IMAP server",
                        action="store_true")
    parser.add_argument("--src-user",
                        help="user name on source IMAP server",
                        required=True, metavar="USERNAME")
    parser.add_argument("--src-password",
                        help="password on source IMAP server",
                        metavar="PASSWORD")

    parser.add_argument("--dest-host",
                        help="hostname of destination IMAP server",
                        default="localhost", metavar="HOSTNAME")
    parser.add_argument("--dest-port",
                        help="port of destination IMAP server",
                        default=None, metavar="PORT")
    parser.add_argument("--dest-no-ssl",
                        help="connect without SSL/TLS on destination IMAP server",
                        action="store_true")
    parser.add_argument("--dest-user",
                        help="user name on destination IMAP server",
                        metavar="USERNAME")
    parser.add_argument("--dest-password",
                        help="password on destination IMAP server",
                        metavar="PASSWORD")

    parser.add_argument("--include", "-i",
                        type=lambda x: ('+', x), action="append", dest="filters",
                        metavar="PATTERN",
                        help="Include matching source folders in the list to be synced")
    parser.add_argument("--exclude", "-e",
                        type=lambda x: ('-', x), action="append", dest="filters",
                        metavar="PATTERN",
                        help="Exclude matching source folders in the list to be synced")
    parser.add_argument("--no-inbox", "-n",
                        const=('-', 'INBOX'), action="append_const", dest="filters",
                        help="Exclude INBOX from the list of folders to be synced")

    parser.add_argument("--dry-run", "-D", action="store_true",
                        help="Perform all steps except for creating mailboxes and writing messages")

    args = parser.parse_args()

    if args.dest_user is None:
        args.dest_user = args.src_user

    if args.src_password is None:
        args.src_password = getpass("Source server password:")
    if args.dest_password is None:
        args.dest_password = getpass("Destination server password:")

    src_info = ServerInfo(args.src_host, args.src_port,
                          args.src_user, args.src_password,
                          not args.src_no_ssl)
    dest_info = ServerInfo(args.dest_host, args.dest_port,
                           args.dest_user, args.dest_password,
                           not args.dest_no_ssl)

    folder_filter = functools.partial(_folder_matcher, args.filters)

    try:
        imap_sync(
            src_info, dest_info,
            progress_class=tqdm,
            folder_filter=folder_filter,
            dry_run=args.dry_run,
        )
    except KeyboardInterrupt:
        print("Folder sync interrupted by user.")


if __name__ == "__main__":
    main()
