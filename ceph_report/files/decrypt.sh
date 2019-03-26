#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

readonly SRC_ENC_FILE="${1}"
readonly DST_FILE="${2}"
readonly ENC_PASSWORD_FILE="${3}"
readonly RSA_KEY_FILE="${4}"

readonly PASSWORD=$(cat "${ENC_PASSWORD_FILE}" | \
                    base64 --wrap=0 --decode | \
                    openssl rsautl -decrypt -inkey "${RSA_KEY_FILE}")

echo "${PASSWORD}" | openssl inc -d -aes-256-cbc -in "${SRC_ENC_FILE}" -out "${DST_FILE}" -pass stdin