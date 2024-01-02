# Copyright 2023 WHERE TRUE Technologies.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import warnings
import functools


def deprecated(cls):
    """Class decorator to mark a class as deprecated."""
    orig_init = cls.__init__

    @functools.wraps(orig_init)
    def new_init(self, *args, **kwargs):
        # pylint: disable=line-too-long
        error_msg = f"{cls.__name__} is being deprecated, please use a table function via the session.\nSee https://www.wheretrue.dev/docs/exon/exondb/api-reference/table-functions for more info."

        warnings.warn(error_msg, category=DeprecationWarning, stacklevel=2)
        orig_init(self, *args, **kwargs)

    cls.__init__ = new_init
    return cls
