"""`python -m baski_lint <files_or_dirs...> [--recursive]`."""

import sys

from baski_lint.anon import main

sys.exit(main())
