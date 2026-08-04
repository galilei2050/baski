"""`python -m baski.lint <files_or_dirs...> [--recursive]`."""

import sys

from baski.lint.anon import main

sys.exit(main())
