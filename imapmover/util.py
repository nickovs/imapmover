# Copyright 2021 Nicko van Someren
#
# Licensed under the Apache License, Version 2.0 (the "License")
# See the LICENSE.txt file for details

# SPDX-License-Identifier: Apache-2.0

"""Utility functions and classes"""


class DummyProgress:
    """A dummy stub when progress indication is not needed"""
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        return False

    def set_description(self, *args, **kwargs):
        pass

    def reset(self, *args, **kwargs):
        pass

    def update(self, *args, **kwargs):
        pass

    def set_postfix_str(self, *args, **kwargs):
        pass
