#!/bin/bash
echo running RmSpaces3.sh

set -e

find . -depth -name '* *' \
| while IFS='' read -r f ; do mv -i "$f" "$(dirname "$f")/$(basename "$f"|tr ' ' _)"

done