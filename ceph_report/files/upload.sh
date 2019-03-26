#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

readonly ARCH="${1}"
readonly ARCH_ENC="${2}"
readonly PUB_KEY="${3}"
readonly CERT="${4}"
readonly URL="${5}"

readonly ARCH_FILE_NAME=$(basename "${ARCH}")
readonly SERVER_FILE_NAME="${ARCH_FILE_NAME}.enc"

if [[ -t 0 ]] ; then
    echo "Enter http_user:http_passwd"
fi

read -r USER_PASSWD

KEY=$(openssl rand -hex 64)
echo "${KEY}" | openssl enc -aes-256-cbc -salt -in "${ARCH}" -out "${ARCH_ENC}" -pass stdin
ENC_KEY=$(echo "${KEY}" | openssl rsautl -encrypt -inkey "${PUB_KEY}" -pubin  | base64 --wrap=0)
curl --silent --show-error --fail -k --cacert "${CERT}" \
     -u "${USER_PASSWD}" \
     --header "Enc-Password: ${ENC_KEY}" \
     --header "Arch-Name: ${SERVER_FILE_NAME}" \
     -X PUT -F "file=@${ARCH_ENC}" "${URL}"
