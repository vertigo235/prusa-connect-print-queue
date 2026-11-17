#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <release-directory>" >&2
  exit 2
fi

if command -v sha256sum >/dev/null 2>&1; then
  hash='sha256sum'
else
  hash='shasum -a 256'
fi

cd "$1"
find . -type f ! -name checksums.txt ! -name checksums.txt.sig \
  | sed 's|^./||' | LC_ALL=C sort \
  | while IFS= read -r file; do
      $hash "$file"
    done > checksums.txt
