# Copyright 2021 Nicko van Someren
#
# Licensed under the Apache License, Version 2.0 (the "License")
# See the LICENSE.txt file for details

# SPDX-License-Identifier: Apache-2.0

"""Core imapmover functions and classes"""

from dataclasses import dataclass

import imapclient

from .util import DummyProgress


# Number of messages about which to get details in any given fetch
MSG_CHUNK_SIZE = 1000
# The IMAP header filter for fetch that identifies a pre-existing message
MSG_ID_HEADERS = b"BODY[HEADER.FIELDS (MESSAGE-ID DATE)]"
MSG_SIZE = b"RFC822.SIZE"
MSG_FLAGS = b'FLAGS'
MSG_RFC822 = b"RFC822"
MSG_DATE = b'INTERNALDATE'


@dataclass
class ServerInfo:
    hostname: str
    port: int
    username: str
    password: str
    SSL: bool


# Determine chunking of messages to move
# Yields lists of message IDs
def _chunk_messages(messages, max_size=(4 << 20)):
    chunk = []
    chunk_total = 0
    for msg_id, details in messages.items():
        if chunk_total + details[MSG_SIZE] >= max_size:
            if chunk:
                yield chunk
                chunk = [msg_id]
                chunk_total = details[MSG_SIZE]
            else:
                yield [msg_id]
        else:
            chunk.append(msg_id)
            chunk_total += details[MSG_SIZE]
    if chunk:
        yield chunk


# This is the function that does the synchronisation work. The Outer function mostly handles setup.
def _imap_sync_core(
        src, dest,
        progress, progress_class,
        replace_sep: str = '_', folder_filter=None,
        dry_run: bool = False,
        ):
    progress.set_description("Finding source folders")
    # List folders on src
    src_folder_data = src.list_folders()
    src_dir_separator = src_folder_data[0][1].decode("ASCII")

    progress.set_description("Checking destination folders")
    # List folders on dest
    dest_folder_data = dest.list_folders()
    dest_dir_separator = dest_folder_data[0][1].decode("ASCII")

    if src_dir_separator != dest_dir_separator:
        def fix_path(path):
            return path.replace(dest_dir_separator, replace_sep).replace(src_dir_separator, dest_dir_separator)
    else:
        def fix_path(path):
            return path

    dir_map = [(path, flags, fix_path(path)) for flags, _, path in src_folder_data]
    # Sorting is not strictly necessary, but it makes log output prettier
    dir_map.sort()

    # Filter source folder list
    if folder_filter is not None:
        all_names = [path for path, _, _ in dir_map]
        filtered_names = folder_filter(all_names)
        dir_map = [(path, flags, fixed_path)
                   for path, flags, fixed_path in dir_map
                   if path in filtered_names]

    # Identify missing folders
    dest_folder_set = set(fixed_path for _, _, fixed_path in dest_folder_data)
    missing_folders = [(fixed_path, flags) for path, flags, fixed_path in dir_map if fixed_path not in dest_folder_set]

    if missing_folders:
        # Create target folders
        progress.set_description("Creating destination folders")
        progress.reset(total=len(missing_folders))
        for folder_path, flags in missing_folders:
            if not dry_run:
                dest.create_folder(folder_path)
            progress.update()

    progress.set_description("Copying messages")
    progress.reset(total=len(dir_map))

    with progress_class(desc="Message data", leave=False) as inner_progress:
        # For each source folder
        for path, flags, fixed_path in dir_map:
            parts = path.split(src_dir_separator)
            progress.set_postfix_str('> '*(len(parts)-1) + parts[-1])
            progress.update()

            # Fetch list of message IDs in target
            if not dry_run or fixed_path not in missing_folders:
                dest_folder_info = dest.select_folder(fixed_path)
                dest_flag_set = set(dest_folder_info[MSG_FLAGS])
                dest_messages = dest.search(['NOT', 'DELETED'])
            else:
                dest_messages = []

            # Fetch the headers for all the messages, in batches
            dest_msg_set = set()
            for i in range(-(-len(dest_messages)//MSG_CHUNK_SIZE)):
                chunk_ids = dest_messages[i*MSG_CHUNK_SIZE:(i+1)*MSG_CHUNK_SIZE]
                info = dest.fetch(chunk_ids, MSG_ID_HEADERS)
                for details in info.values():
                    dest_msg_set.add(details[MSG_ID_HEADERS])

            data_total = 0

            # Find the IDs of all the source messages
            src.select_folder(path)
            src_messages = src.search(['NOT', 'DELETED'])
            # Fetch the headers for all the messages, in batches
            move_messages = {}
            for i in range(-(-len(src_messages)//MSG_CHUNK_SIZE)):
                chunk_ids = src_messages[i*MSG_CHUNK_SIZE:(i+1)*MSG_CHUNK_SIZE]
                info = src.fetch(chunk_ids, [MSG_ID_HEADERS, MSG_SIZE])
                for msg_id, details in info.items():
                    if details[MSG_ID_HEADERS] not in dest_msg_set:
                        move_messages[msg_id] = details
                        data_total += details[MSG_SIZE]

            inner_progress.reset(total=data_total)

            # For each chunk of message
            for chunk_ids in _chunk_messages(move_messages, max_size=(4 << 20)):
                # Fetch chunk of messages from src
                msgs = src.fetch(chunk_ids, [MSG_FLAGS, MSG_DATE, MSG_SIZE, MSG_RFC822])
                # For each message in chunk
                for msg in msgs.values():
                    # Make sure that the destination supports the message flags
                    flags = [f for f in msg[MSG_FLAGS] if f in dest_flag_set]
                    # Append message to dest
                    dest.append(fixed_path, msg[MSG_RFC822], flags, msg[MSG_DATE])
                    inner_progress.update(n=msg[MSG_SIZE])


def imap_sync(
        src_info: ServerInfo, dest_info: ServerInfo,
        replace_sep: str = '_', progress_class=None,
        folder_filter=None, dry_run=False,
        ):
    """Sync folders and messages from src to dest"""

    if progress_class is None:
        progress_class = DummyProgress

    # Make sure that everything that needs clean-up has a context manager.
    with progress_class(desc="Connecting to source server") as progress:
        with imapclient.IMAPClient(host=src_info.hostname, port=src_info.port, ssl=src_info.SSL) as src:
            src.login(src_info.username, src_info.password)

            progress.set_description("Connecting to destination server")

            with imapclient.IMAPClient(host=dest_info.hostname, port=dest_info.port, ssl=dest_info.SSL) as dest:
                dest.login(dest_info.username, dest_info.password)
                # The real work is done in the _imap_sync_core function above
                _imap_sync_core(
                    src, dest,
                    progress=progress, progress_class=progress_class,
                    replace_sep=replace_sep, folder_filter=folder_filter,
                    dry_run=dry_run
                )
